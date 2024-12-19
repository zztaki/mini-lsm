#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::ops::Bound;

use anyhow::{Ok, Result};
use bytes::Bytes;
use nom::Err;

use crate::{
    iterators::{
        concat_iterator::SstConcatIterator, merge_iterator::MergeIterator,
        two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::{map_bound, MemTableIterator},
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the tutorial for multiple times.
// type LsmIteratorInner =
//     TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;
type MergeL0Interator = MergeIterator<SsTableIterator>;
type LsmIteratorInner = TwoMergeIterator<
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeL0Interator>,
    SstConcatIterator,
>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<&[u8]>) -> Result<Self> {
        let mut lsm_iter = Self {
            inner: iter,
            upper: map_bound(upper),
        };
        while lsm_iter.is_valid() && lsm_iter.value().is_empty() {
            lsm_iter.next()?;
        }
        Ok(lsm_iter)
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        // unimplemented!()
        self.inner.is_valid()
            && match self.upper {
                Bound::Included(ref upper) => self.key() <= upper,
                Bound::Excluded(ref upper) => self.key() < upper,
                Bound::Unbounded => true,
            }
    }

    fn key(&self) -> &[u8] {
        // unimplemented!()
        &self.inner.key().raw_ref()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        self.inner.value()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        println!("{:?}", self.key());
        self.inner.next()?;
        while self.is_valid() && self.value().is_empty() {
            self.inner.next()?;
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        // unimplemented!()
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        // unimplemented!()
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        // unimplemented!()
        self.iter.key()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        self.iter.value()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        if self.has_errored {
            return Err(anyhow::anyhow!("FusedIterator: Iterator has errored"));
        }
        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        // unimplemented!()
        self.iter.num_active_iterators()
    }
}
