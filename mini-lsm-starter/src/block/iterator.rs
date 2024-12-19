#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        let mut iter = Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        };
        if !iter.block.offsets.is_empty() {
            iter.first_key = iter.get_key(0);
        }
        iter
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        // unimplemented!()
        let mut iter = Self::new(block);
        if iter.block.offsets.is_empty() {
            return iter;
        }
        iter.seek_to_first();
        iter
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        // unimplemented!()
        let mut iter = Self::new(block);
        if iter.block.offsets.is_empty() {
            return iter;
        }
        iter.seek_to_key(key);
        iter
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        // unimplemented!()
        KeySlice::from_slice(self.key.raw_ref())
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        // unimplemented!()
        &self.block.data[self.value_range.0..self.value_range.1]
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        // unimplemented!()
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        // unimplemented!()
        self.idx = 0;
        self.key = self.first_key.clone();

        let value_range_start = 6 + self.first_key.len();
        let value_range_end = value_range_start
            + u16::from_be_bytes([
                self.block.data[value_range_start - 2],
                self.block.data[value_range_start - 1],
            ]) as usize;

        // key_len and valu_len is u16
        self.value_range = (value_range_start, value_range_end);
    }

    fn get_overlap_len(&self, entry_offset: usize) -> usize {
        u16::from_be_bytes([
            self.block.data[entry_offset],
            self.block.data[entry_offset + 1],
        ]) as usize
    }

    fn get_rest_len(&self, entry_offset: usize) -> usize {
        u16::from_be_bytes([
            self.block.data[entry_offset + 2],
            self.block.data[entry_offset + 3],
        ]) as usize
    }

    fn get_key(&self, entry_offset: usize) -> KeyVec {
        let mut current = KeyVec::new();
        current.append(&self.first_key.raw_ref()[..self.get_overlap_len(entry_offset)]);
        current.append(
            &self.block.data[entry_offset + 4..entry_offset + 4 + self.get_rest_len(entry_offset)],
        );
        current
    }

    fn set_value_range(&mut self, entry_offset: usize) {
        let rest_len = self.get_rest_len(entry_offset);
        let value_len = u16::from_be_bytes([
            self.block.data[entry_offset + 4 + rest_len],
            self.block.data[entry_offset + 4 + rest_len + 1],
        ]) as usize;
        self.value_range = (
            entry_offset + 4 + rest_len + 2,
            entry_offset + 4 + rest_len + 2 + value_len,
        );
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        // unimplemented!()
        self.idx += 1;
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        let new_entry_start = self.value_range.1;

        self.key = self.get_key(new_entry_start);

        self.set_value_range(new_entry_start);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // unimplemented!()
        // binary search
        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left < right {
            let mid = left + (right - left) / 2;
            let entry_offset = self.block.offsets[mid] as usize;
            let mid_key = self.get_key(entry_offset);
            if mid_key < key.to_key_vec() {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        self.idx = left;
        if self.idx >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return;
        }

        // create self.key and self.value_range
        let current_entry_start = self.block.offsets[self.idx] as usize;
        let overlap_len = self.get_overlap_len(current_entry_start);
        let rest_len = self.get_rest_len(current_entry_start);
        self.key = self.get_key(current_entry_start);

        let value_len = u16::from_be_bytes([
            self.block.data[current_entry_start + 4 + rest_len],
            self.block.data[current_entry_start + 4 + rest_len + 1],
        ]) as usize;
        self.value_range = (
            current_entry_start + 4 + rest_len + 2,
            current_entry_start + 4 + rest_len + 2 + value_len,
        );
    }
}
