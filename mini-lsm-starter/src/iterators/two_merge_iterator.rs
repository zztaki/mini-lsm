use anyhow::{Ok, Result};

use super::StorageIterator;
use crate::key::KeySlice;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    current_is_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn move_to_exist(&mut self) -> Result<()> {
        // unimplemented!()
        while self.a.is_valid() && self.a.value().is_empty() {
            self.a.next()?;
        }
        while self.b.is_valid() && self.b.value().is_empty() {
            self.b.next()?;
        }
        if self.a.is_valid() && self.b.is_valid() {
            if self.a.key() == self.b.key() {
                self.current_is_a = true;
                self.b.next()?;
            } else if self.a.key() < self.b.key() {
                self.current_is_a = true;
            } else {
                self.current_is_a = false;
            }
        } else if self.a.is_valid() {
            self.current_is_a = true;
        } else {
            self.current_is_a = false;
        }
        Ok(())
    }

    pub fn create(a: A, b: B) -> Result<Self> {
        // unimplemented!()
        let mut iter = Self {
            a,
            b,
            current_is_a: true,
        };
        iter.move_to_exist()?;
        Ok(iter)
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        // unimplemented!()
        if !self.is_valid() {
            panic!("Invalid iterator");
        }
        if self.current_is_a {
            self.a.key()
        } else {
            self.b.key()
        }
    }

    fn value(&self) -> &[u8] {
        // unimplemented!()
        if self.current_is_a {
            self.a.value()
        } else {
            self.b.value()
        }
    }

    fn is_valid(&self) -> bool {
        // unimplemented!()
        self.a.is_valid() || self.b.is_valid()
    }

    fn next(&mut self) -> Result<()> {
        // unimplemented!()
        if self.current_is_a {
            self.a.next()?;
        } else {
            self.b.next()?;
        }
        self.move_to_exist()?;
        Ok(())
    }
}
