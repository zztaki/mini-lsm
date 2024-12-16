#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod builder;
mod iterator;

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

        let num_of_entries = u16::from(data[data.len() - 2]) << 8 | u16::from(data[data.len() - 1]);
        for i in 0..num_of_entries {
            let offset = u16::from(data[data.len() - 4 - 2 * (i as usize)]) << 8
                | u16::from(data[data.len() - 3 - 2 * (i as usize)]);
            offsets.push(offset);
        }
        offsets.reverse();

        let kv_data = data[..data.len() - 2 - 2 * (num_of_entries as usize)].to_vec();

        Self {
            offsets,
            data: kv_data,
        }
    }
}
