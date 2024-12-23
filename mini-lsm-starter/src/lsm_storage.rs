#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

use std::collections::HashMap;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use anyhow::{Ok, Result};
use bytes::Bytes;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{self, FusedIterator, LsmIterator};
use crate::manifest::Manifest;
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        // unimplemented!()
        self.flush_notifier.send(()).ok();
        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir_all(path)?;
        }
        let state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache: Arc::new(BlockCache::new(1024)),
            next_sst_id: AtomicUsize::new(1),
            compaction_controller,
            manifest: None,
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, _key: &[u8]) -> Result<Option<Bytes>> {
        // unimplemented!()
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        if let Some(res) = snapshot.memtable.get(_key) {
            // in memtable
            return Ok(Some(res).filter(|v| !v.is_empty()));
        }

        for imm_memtable in &snapshot.imm_memtables {
            if let Some(res) = imm_memtable.get(_key) {
                // in imm_memtable
                return Ok(Some(res).filter(|v| !v.is_empty()));
            }
        }

        let key_within_sst = |sst: &Arc<SsTable>| {
            let first_key = sst.first_key().raw_ref();
            let last_key = sst.last_key().raw_ref();
            first_key <= _key && _key <= last_key
        };

        let check_bloom = |sst: &Arc<SsTable>| {
            let bloom = sst.bloom.as_ref().unwrap();
            let key = Bytes::copy_from_slice(_key);
            bloom.may_contain(farmhash::fingerprint32(&key))
        };

        let l0_iters = snapshot
            .l0_sstables
            .iter()
            .filter_map(|sst_id| {
                let sst = &snapshot.sstables[sst_id];
                (key_within_sst(sst) && check_bloom(sst)).then(|| {
                    let table = Arc::clone(sst);
                    let iter =
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(_key));
                    iter.map(Box::new)
                })
            })
            .collect::<Result<_>>()?;
        let l0_merge_iter = MergeIterator::create(l0_iters);
        if l0_merge_iter.is_valid() && l0_merge_iter.key().raw_ref() == _key {
            return Ok(
                Some(Bytes::copy_from_slice(l0_merge_iter.value())).filter(|v| !v.is_empty())
            );
        }

        for level in &snapshot.levels {
            // TODO: binary search
            for sst_id in &level.1 {
                let sst = snapshot.sstables.get(sst_id).unwrap();
                if sst.first_key().raw_ref() > _key {
                    break;
                } else if check_bloom(sst) {
                    let table = Arc::clone(sst);
                    let iter =
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(_key))?;
                    if iter.is_valid() && iter.key().raw_ref() == _key {
                        return Ok(
                            Some(Bytes::copy_from_slice(iter.value())).filter(|v| !v.is_empty())
                        );
                    }
                }
            }
        }
        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, _key: &[u8], _value: &[u8]) -> Result<()> {
        // unimplemented!()
        let key = Bytes::copy_from_slice(_key);
        let value = Bytes::copy_from_slice(_value);
        // As our memtable implementation only requires an immutable reference for put,
        // you ONLY need to take the read lock on state in order to modify the memtable.
        // This allows concurrent access to the memtable from multiple threads.
        self.state.read().memtable.put(&key, &value)?;

        if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
            let state_lock = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() >= self.options.target_sst_size {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, _key: &[u8]) -> Result<()> {
        // unimplemented!()
        let value = &[];
        self.put(_key, value)
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        unimplemented!()
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        // unimplemented!()
        // let memtable = MemTable::create_with_wal(self.next_sst_id(), self.path_of_wal(0))?;
        let mut memtable = Arc::new(MemTable::create(self.next_sst_id()));
        {
            // TODO: :( Rust :(
            // cannot borrow data in an `Arc` as mutable
            // trait `DerefMut` is required to modify through a dereference, but it is not implemented for `std::sync::Arc<LsmStorageState>`
            // let mut state = self.state.write();
            // state.imm_memtables.insert(0, state.memtable.clone());
            // state.memtable.clone_from(&memtable);

            let mut state_guard = self.state.write();
            let mut new_state = state_guard.as_ref().clone();
            std::mem::swap(&mut memtable, &mut new_state.memtable);
            new_state.imm_memtables.insert(0, memtable);
            *state_guard = Arc::new(new_state);
        }
        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        // unimplemented!()
        let _state_lock = self.state_lock.lock();

        let memtable_to_flush;
        let snapshot = {
            let guard = self.state.read();
            memtable_to_flush = guard.imm_memtables.last().unwrap().clone();
        };

        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        memtable_to_flush.flush(&mut sst_builder)?;
        let ss_table = sst_builder.build(
            memtable_to_flush.id(),
            Some(Arc::clone(&self.block_cache)),
            self.path_of_sst(memtable_to_flush.id()),
        )?;
        {
            let mut guard = self.state.write();
            let mut new_state = guard.as_ref().clone();
            new_state.l0_sstables.insert(0, memtable_to_flush.id());
            new_state
                .sstables
                .insert(memtable_to_flush.id(), Arc::new(ss_table));
            new_state.imm_memtables.pop();
            *guard = Arc::new(new_state);
        }
        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        _lower: Bound<&[u8]>,
        _upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        // unimplemented!()

        let snapshot = {
            let snapshot = self.state.read();
            Arc::clone(&snapshot)
        };

        let mem_iters = std::iter::once(&snapshot.memtable)
            .chain(snapshot.imm_memtables.iter())
            .map(|memtable| Box::new(memtable.scan(_lower, _upper)))
            .collect();
        let mem_merge_iter = MergeIterator::create(mem_iters);

        let range_overlap_with_sst = move |sst: &Arc<SsTable>| {
            let first_key = sst.first_key().raw_ref();
            let last_key = sst.last_key().raw_ref();
            match _lower {
                Bound::Included(left) if last_key < left => return false,
                Bound::Excluded(left) if last_key <= left => return false,
                _ => (),
            }
            match _upper {
                Bound::Included(right) if right < first_key => return false,
                Bound::Excluded(right) if right <= first_key => return false,
                _ => (),
            }
            true
        };

        let l0_iters = snapshot
            .l0_sstables
            .iter()
            .filter_map(|sst_id| {
                let sst = &snapshot.sstables[sst_id];
                range_overlap_with_sst(sst).then(move || {
                    let table = Arc::clone(sst);
                    let iter = match _lower {
                        Bound::Included(key) => SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key),
                        ),
                        Bound::Excluded(key) => SsTableIterator::create_and_seek_to_key(
                            table,
                            KeySlice::from_slice(key),
                        )
                        .and_then(|mut iter| {
                            if iter.is_valid() && iter.key() == KeySlice::from_slice(key) {
                                iter.next()?;
                            }
                            Ok(iter)
                        }),
                        Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table),
                    };
                    iter.map(Box::new)
                })
            })
            .collect::<Result<_>>()?;
        let l0_merge_iter = MergeIterator::create(l0_iters);

        let key_slice = match _lower {
            Bound::Included(x) => KeySlice::from_slice(x),
            Bound::Excluded(x) => KeySlice::from_slice(x),
            Bound::Unbounded => KeySlice::default(),
        };
        let mut level_concat_iters: Vec<Box<SstConcatIterator>> = Vec::new();
        for (_, ids) in &snapshot.levels {
            let mut ssts = Vec::new();
            for id in ids {
                let sst = snapshot.sstables.get(id).unwrap();
                if range_overlap_with_sst(sst) {
                    ssts.push(sst.clone());
                }
            }

            let mut level_concat_iter = SstConcatIterator::create_and_seek_to_key(ssts, key_slice)?;
            if let Bound::Excluded(x) = _lower {
                if x == key_slice.raw_ref() {
                    level_concat_iter.next()?;
                }
            }
            level_concat_iters.push(Box::new(level_concat_iter));
        }

        Ok(FusedIterator::new(LsmIterator::new(
            TwoMergeIterator::create(
                TwoMergeIterator::create(mem_merge_iter, l0_merge_iter)?,
                MergeIterator::create(level_concat_iters),
            )?,
            _upper,
        )?))
    }
}
