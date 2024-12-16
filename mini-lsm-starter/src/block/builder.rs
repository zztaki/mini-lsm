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
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        // unimplemented!()
        let key_len = key.len();
        let value_len = value.len();
        if !self.data.is_empty()
            && (self.data.len() + self.offsets.len() + key_len + value_len + 4) > self.block_size
        {
            return false;
        }
        self.offsets.push(self.data.len() as u16);
        self.data.extend(&(key_len as u16).to_be_bytes());
        self.data.extend_from_slice(&key.raw_ref());
        self.data.extend(&(value_len as u16).to_be_bytes());
        self.data.extend_from_slice(value);
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
}
