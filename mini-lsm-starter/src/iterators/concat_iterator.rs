#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use anyhow::{Error, Result};
use nom::Err;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        // unimplemented!()
        let mut iter = Self {
            current: None,
            next_sst_idx: 0,
            sstables,
        };
        if !iter.sstables.is_empty() {
            iter.current = Some(SsTableIterator::create_and_seek_to_first(
                iter.sstables[0].clone(),
            )?);
            iter.next_sst_idx = 1;
        }
        Ok(iter)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        // unimplemented!()
        if sstables.is_empty() {
            return Self::create_and_seek_to_first(sstables);
        }

        let mut iter = Self {
            current: None,
            next_sst_idx: sstables.len(),
            sstables,
        };
        for (idx, sstable) in iter.sstables.iter().enumerate() {
            if key.raw_ref() <= sstable.last_key().raw_ref() {
                iter.current = Some(SsTableIterator::create_and_seek_to_key(
                    sstable.clone(),
                    key,
                )?);
                iter.next_sst_idx = idx + 1;
                break;
            }
        }
        Ok(iter)
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current
            .as_ref()
            .map(SsTableIterator::key)
            .unwrap_or_default()
    }

    fn value(&self) -> &[u8] {
        self.current
            .as_ref()
            .map(SsTableIterator::value)
            .unwrap_or_default()
    }

    fn is_valid(&self) -> bool {
        self.current
            .as_ref()
            .map(SsTableIterator::is_valid)
            .unwrap_or_default()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        if let Some(current) = self.current.as_mut() {
            current.next()?;
        }

        if !self.is_valid() {
            let idx = self.next_sst_idx;
            self.next_sst_idx += 1;

            if let Some(sst) = self.sstables.get(idx) {
                self.current = Some(SsTableIterator::create_and_seek_to_first(sst.clone())?);
            }
        }

        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
