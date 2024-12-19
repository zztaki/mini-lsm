#![allow(dead_code)] // REMOVE THIS LINE after fully implementing this functionality

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;
use std::vec;

use anyhow::{Ok, Result};
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, _task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        // unimplemented!()
        match _task {
            // CompactionTask::Leveled(task) => self.compact_leveled(task),
            // CompactionTask::Simple(task) => self.compact_simple_leveled(task),
            // CompactionTask::Tiered(task) => self.compact_tiered(task),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut sstables = Vec::new();
                let l0_iters = MergeIterator::create(
                    l0_sstables
                        .iter()
                        .map(|id| {
                            Box::new(
                                SsTableIterator::create_and_seek_to_first(
                                    self.state.read().sstables.get(id).unwrap().clone(),
                                )
                                .unwrap(),
                            )
                        })
                        .collect(),
                );
                let l1_iters = SstConcatIterator::create_and_seek_to_first(
                    l1_sstables
                        .iter()
                        .map(|id| self.state.read().sstables.get(id).unwrap().clone())
                        .collect(),
                )?;
                let mut l0_l1_iters = TwoMergeIterator::create(l0_iters, l1_iters)?;
                let mut builder = SsTableBuilder::new(self.options.block_size);
                while l0_l1_iters.is_valid() {
                    if l0_l1_iters.value().is_empty() {
                        l0_l1_iters.next()?;
                        continue;
                    }
                    println!(
                        "key: {:?}, value: {:?}",
                        l0_l1_iters.key(),
                        l0_l1_iters.value()
                    );
                    builder.add(l0_l1_iters.key(), l0_l1_iters.value());
                    if builder.estimated_size() >= self.options.target_sst_size {
                        let sst_id = self.next_sst_id();
                        let sst = builder.build(
                            sst_id,
                            Some(Arc::clone(&self.block_cache)),
                            self.path_of_sst(sst_id),
                        )?;
                        sstables.push(Arc::new(sst));
                        builder = SsTableBuilder::new(self.options.block_size);
                    }
                    l0_l1_iters.next()?;
                }
                if !builder.is_empty() {
                    let sst_id = self.next_sst_id();
                    let sst = builder.build(
                        sst_id,
                        Some(self.block_cache.clone()),
                        self.path_of_sst(sst_id),
                    )?;
                    sstables.push(Arc::new(sst));
                }
                Ok(sstables)
                // let mut sstables = Vec::new();
                // let mut l0_l1_iters = MergeIterator::create(
                //     l0_sstables
                //         .iter()
                //         .chain(l1_sstables.iter())
                //         .map(|id| {
                //             Box::new(
                //                 SsTableIterator::create_and_seek_to_first(
                //                     self.state.read().sstables.get(id).unwrap().clone(),
                //                 )
                //                 .unwrap(),
                //             )
                //         })
                //         .collect(),
                // );
                // let mut builder = SsTableBuilder::new(self.options.block_size);
                // while l0_l1_iters.is_valid() {
                //     if l0_l1_iters.value().is_empty() {
                //         l0_l1_iters.next()?;
                //         continue;
                //     }
                //     builder.add(l0_l1_iters.key(), l0_l1_iters.value());
                //     if builder.estimated_size() >= self.options.target_sst_size {
                //         let sst_id = self.next_sst_id();
                //         let sst = builder.build(
                //             sst_id,
                //             Some(Arc::clone(&self.block_cache)),
                //             self.path_of_sst(sst_id),
                //         )?;
                //         sstables.push(Arc::new(sst));
                //         builder = SsTableBuilder::new(self.options.block_size);
                //     }
                //     l0_l1_iters.next()?;
                // }
                // if !builder.is_empty() {
                //     let sst_id = self.next_sst_id();
                //     let sst = builder.build(
                //         sst_id,
                //         Some(self.block_cache.clone()),
                //         self.path_of_sst(sst_id),
                //     )?;
                //     sstables.push(Arc::new(sst));
                // }
                // Ok(sstables)
            }
            _ => unimplemented!(),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        // unimplemented!()
        // self.state.read().levels
        let (l0_sst_ids, l1_sst_ids) = {
            let state = self.state.read();
            (state.l0_sstables.clone(), state.levels[0].1.clone())
        };
        let new_ssts = self.compact(&CompactionTask::ForceFullCompaction {
            l0_sstables: l0_sst_ids.clone(),
            l1_sstables: l1_sst_ids.clone(),
        })?;
        {
            let _state_lock = self.state_lock.lock();
            let mut state = self.state.write();
            let mut snapshot = state.as_ref().clone();

            // 从 snapshot sstables 中删除 l0 和 l1 原有 sst
            let _ = l0_sst_ids.iter().map(|id| snapshot.sstables.remove(id));
            let _ = l1_sst_ids.iter().map(|id| snapshot.sstables.remove(id));
            // l0 和 l1 compaction 后作为一个 sorted run 加入 l1
            snapshot.l0_sstables.retain(|id| !l0_sst_ids.contains(id)); // 在压缩过程中，可能会有新的 sst 文件加入 l0，这部分需要保留
            snapshot.levels[0].1 = new_ssts.iter().map(|sst| sst.sst_id()).collect();
            new_ssts.iter().for_each(|sst| {
                snapshot.sstables.insert(sst.sst_id(), sst.clone());
            });

            *state = Arc::new(snapshot);
        }
        // remove the sst files that have been compacted
        for sst in l0_sst_ids.iter().chain(l1_sst_ids.iter()) {
            std::fs::remove_file(self.path_of_sst(*sst))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let guard = self.state.read();
        if guard.imm_memtables.len() >= self.options.num_memtable_limit - 1 {
            drop(guard);
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
