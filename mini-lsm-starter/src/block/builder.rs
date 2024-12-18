#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
    /// The last key in the block
    last_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        // unimplemented!()
        Self {
            offsets: Vec::new(),
            data: Vec::new(),
            block_size,
            first_key: KeyVec::new(),
            last_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // unimplemented!()
        let key_len = key.len();
        let value_len = value.len();
        let mut common_prefix_len = 0;
        while common_prefix_len < key_len && common_prefix_len < self.first_key.len() {
            if key.raw_ref()[common_prefix_len] != self.first_key.raw_ref()[common_prefix_len] {
                break;
            }
            common_prefix_len += 1;
        }
        let increase_size = 8 + key_len - common_prefix_len + value_len;

        if !self.data.is_empty() && (self.data.len() + increase_size) > self.block_size {
            return false;
        }

        self.offsets.push(self.data.len() as u16);
        self.data.extend(&(common_prefix_len as u16).to_be_bytes());
        self.data
            .extend(&(key_len as u16 - common_prefix_len as u16).to_be_bytes());
        self.data
            .extend_from_slice(&key.raw_ref()[common_prefix_len..]);
        self.data.extend(&(value_len as u16).to_be_bytes());
        self.data.extend_from_slice(value);
        if self.first_key.is_empty() {
            self.first_key.append(key.raw_ref());
        }
        self.last_key.clear();
        self.last_key.append(key.raw_ref());
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        // unimplemented!()
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        // unimplemented!()
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    pub fn first_key(&self) -> KeyVec {
        self.first_key.clone()
    }

    pub fn last_key(&self) -> KeyVec {
        self.last_key.clone()
    }
}
