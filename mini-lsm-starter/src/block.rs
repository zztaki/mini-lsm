#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

use std::io::Read;

use anyhow::Ok;
pub use builder::BlockBuilder;
use bytes::Bytes;
pub use iterator::BlockIterator;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the tutorial
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        // unimplemented!()
        let num_of_entries = self.offsets.len();
        let mut encoded = Vec::new();

        encoded.extend(&self.data);
        for offset in &self.offsets {
            encoded.extend(&offset.to_be_bytes());
        }
        encoded.extend(&(num_of_entries as u16).to_be_bytes());
        Bytes::copy_from_slice(&encoded)
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        // unimplemented!()
        if data.is_empty() {
            return Self {
                offsets: Vec::new(),
                data: Vec::new(),
            };
        }
        let mut offsets = Vec::new();

        let num_of_entries =
            u16::from_be_bytes(data[data.len() - 2..data.len()].try_into().unwrap());
        for i in 0..num_of_entries {
            offsets.push(u16::from_be_bytes(
                data[data.len() - 4 - 2 * (i as usize)..data.len() - 2 - 2 * (i as usize)]
                    .try_into()
                    .unwrap(),
            ));
        }
        offsets.reverse();

        let kv_data = data[..data.len() - 2 - 2 * (num_of_entries as usize)].to_vec();

        Self {
            offsets,
            data: kv_data,
        }
    }

    pub fn first_key(&self) -> &[u8] {
        // unimplemented!()
        if self.offsets.is_empty() {
            panic!("Empty block");
        }
        let first_key_len = u16::from_be_bytes([self.data[2], self.data[3]]) as usize;
        &self.data[4..4 + first_key_len]
    }
}
