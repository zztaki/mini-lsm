#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::cmp::{self};
use std::collections::{BinaryHeap, HashMap};

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        // unimplemented!()
        let mut heap = BinaryHeap::new();
        for (i, iter) in iters.into_iter().enumerate() {
            if iter.is_valid() {
                heap.push(HeapWrapper(i, iter));
            }
        }
        let current = heap.pop();
        MergeIterator {
            iters: heap,
            current,
        }
    }
}

impl<I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>> StorageIterator
    for MergeIterator<I>
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        // unimplemented!()
        self.current.as_ref().unwrap().1.key()
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        self.current.as_ref().unwrap().1.value()
    }

    fn is_valid(&self) -> bool {
        // unimplemented!()
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        let mut old_top = self.current.take().unwrap();
        debug_assert!(!self.is_valid());

        let key = old_top.1.key();
        while let Some(mut top) = self.iters.pop() {
            if key != top.1.key() {
                debug_assert!(top.1.is_valid());
                self.iters.push(top);
                break;
            }

            top.1.next()?;
            if top.1.is_valid() {
                self.iters.push(top);
            }
        }

        old_top.1.next()?;
        if old_top.1.is_valid() {
            self.iters.push(old_top);
        }

        self.current = self.iters.pop();
        Ok(())
    }
}
