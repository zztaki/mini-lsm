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
            let first_key_len =
                u16::from_be_bytes([iter.block.data[0], iter.block.data[1]]) as usize;
            iter.first_key
                .append(&iter.block.data[2..2 + first_key_len]); // key_len is u16
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

        let value_range_start = 4 + self.first_key.len();
        let value_range_end = value_range_start
            + u16::from_be_bytes([
                self.block.data[value_range_start - 2],
                self.block.data[value_range_start - 1],
            ]) as usize;

        // key_len and valu_len is u16
        self.value_range = (value_range_start, value_range_end);
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
        let key_len = u16::from_be_bytes([
            self.block.data[new_entry_start],
            self.block.data[new_entry_start + 1],
        ]) as usize;
        self.key.clear();
        self.key
            .append(&self.block.data[new_entry_start + 2..new_entry_start + 2 + key_len]);
        let value_len = u16::from_be_bytes([
            self.block.data[new_entry_start + 2 + key_len],
            self.block.data[new_entry_start + 2 + key_len + 1],
        ]) as usize;
        self.value_range = (
            new_entry_start + 2 + key_len + 2,
            new_entry_start + 2 + key_len + 2 + value_len,
        );
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

            let key_len = u16::from_be_bytes([
                self.block.data[entry_offset],
                self.block.data[entry_offset + 1],
            ]) as usize;
            let mid_key = KeySlice::from_slice(
                &self.block.data[entry_offset + 2..entry_offset + 2 + key_len],
            );
            if mid_key < key {
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
        let key_len = u16::from_be_bytes([
            self.block.data[current_entry_start],
            self.block.data[current_entry_start + 1],
        ]) as usize;
        self.key.clear();
        self.key
            .append(&self.block.data[current_entry_start + 2..current_entry_start + 2 + key_len]);
        let value_len = u16::from_be_bytes([
            self.block.data[current_entry_start + 2 + key_len],
            self.block.data[current_entry_start + 2 + key_len + 1],
        ]) as usize;
        self.value_range = (
            current_entry_start + 2 + key_len + 2,
            current_entry_start + 2 + key_len + 2 + value_len,
        );
    }
}
