#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::sync::Arc;
use std::{io::Read, path::Path};

use anyhow::Result;
use bytes::Bytes;
use nom::AsBytes;

use super::{BlockMeta, FileObject, SsTable};
use crate::{
    block::{self, BlockBuilder},
    key::{KeyBytes, KeySlice},
    lsm_storage::BlockCache,
};

/// Builds an SSTable from key-value pairs.
pub struct SsTableBuilder {
    builder: BlockBuilder,
    first_key: Vec<u8>,
    last_key: Vec<u8>,
    data: Vec<u8>,
    pub(crate) meta: Vec<BlockMeta>,
    block_size: usize,
}

impl SsTableBuilder {
    /// Create a builder based on target block size.
    pub fn new(block_size: usize) -> Self {
        // unimplemented!()
        Self {
            builder: BlockBuilder::new(block_size),
            first_key: Vec::new(),
            last_key: Vec::new(),
            data: Vec::new(),
            meta: Vec::new(),
            block_size,
        }
    }

    pub fn split_block(&mut self) {
        let old_block_builder =
            std::mem::replace(&mut self.builder, BlockBuilder::new(self.block_size));
        if self.first_key.is_empty() {
            self.first_key = old_block_builder.first_key().raw_ref().to_vec();
        }
        self.meta.push(BlockMeta {
            offset: self.data.len(),
            first_key: old_block_builder.first_key().into_key_bytes(),
            last_key: old_block_builder.last_key().into_key_bytes(),
        });
        let block = old_block_builder.build();
        self.data.extend(&block.encode());
    }

    /// Adds a key-value pair to SSTable.
    ///
    /// Note: You should split a new block when the current block is full.(`std::mem::replace` may
    /// be helpful here)
    pub fn add(&mut self, key: KeySlice, value: &[u8]) {
        // unimplemented!()
        self.last_key = key.raw_ref().to_vec();
        if self.builder.add(key, value) {
            return;
        }

        self.split_block();
        let success = self.builder.add(key, value);
        debug_assert!(success);
    }

    /// Get the estimated size of the SSTable.
    ///
    /// Since the data blocks contain much more data than meta blocks, just return the size of data
    /// blocks here.
    pub fn estimated_size(&self) -> usize {
        // unimplemented!()
        self.data.len()
    }

    pub fn encode(&mut self) -> Vec<u8> {
        if !self.builder.is_empty() {
            self.split_block();
        }
        let mut encoded = Vec::with_capacity(self.estimated_size());
        encoded.extend(&self.data);
        BlockMeta::encode_block_meta(&self.meta, &mut encoded);
        encoded.extend((self.data.len() as u32).to_be_bytes());
        encoded
    }

    /// Builds the SSTable and writes it to the given path. Use the `FileObject` structure to manipulate the disk objects.
    pub fn build(
        mut self,
        id: usize,
        block_cache: Option<Arc<BlockCache>>,
        path: impl AsRef<Path>,
    ) -> Result<SsTable> {
        // unimplemented!()
        let file = super::FileObject::create(path.as_ref(), self.encode())?;
        Ok(SsTable {
            file,
            block_meta: self.meta,
            block_meta_offset: self.data.len(),
            id,
            block_cache,
            first_key: KeyBytes::from_bytes(Bytes::copy_from_slice(self.first_key.as_bytes())),
            last_key: KeyBytes::from_bytes(Bytes::copy_from_slice(self.last_key.as_bytes())),
            bloom: None,
            max_ts: 0,
        })
    }

    #[cfg(test)]
    pub(crate) fn build_for_test(self, path: impl AsRef<Path>) -> Result<SsTable> {
        self.build(0, None, path)
    }
}
