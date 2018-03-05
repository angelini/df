use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use rand::{self, Rng};

use block::{Block};
use value::Value;

#[derive(Debug)]
pub enum Error {
    MissingIndex(u64),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::MissingIndex(idx) => write!(f, "Index {} not found in pool", idx),
        }
    }
}

type Result<T> = ::std::result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct Entry {
    pub block: Rc<Block>,
    pub sorted: bool,
}

impl Entry {
    pub fn new(block: Rc<Block>, sorted: bool) -> Entry {
        Entry { block, sorted }
    }
}

#[derive(Debug, Default)]
pub struct Pool {
    entries: HashMap<u64, Entry>,
}

pub type PoolRef = Arc<Mutex<Pool>>;

impl Pool {
    pub fn new_ref() -> PoolRef {
        Arc::new(Mutex::new(Pool::default()))
    }

    pub fn size(&self) -> usize {
        self.entries.len()
    }

    pub fn len(&self, idx: &u64) -> usize {
        self.entries.get(idx).map(|entry| entry.block.len()).unwrap_or(0)
    }

    pub fn is_column_materialized(&self, idx: &u64) -> bool {
        match self.entries.get(idx) {
            Some(_) => true,
            None => false,
        }
    }

    pub fn get_entry(&self, idx: &u64) -> Result<Entry> {
        match self.entries.get(idx) {
            Some(entry) => Ok(entry.clone()),
            None => Err(Error::MissingIndex(*idx)),
        }
    }

    pub fn get_value(&self, col_idx: &u64, row_idx: &u64) -> Option<Value> {
        // FIXME: should accept row_idx as usize
        self.entries.get(col_idx).map(|entry| entry.block.get(*row_idx as usize))
    }

    pub fn set_block(&mut self, idx: u64, block: Rc<Block>, sorted: bool) {
        self.entries.insert(idx, Entry::new(block, sorted));
    }

    pub fn set_initial_block(&mut self, block: Box<Block>) -> u64 {
        let idx = self.unused_idx();
        self.entries.insert(idx, Entry::new(Rc::from(block), false));
        idx
    }

    fn unused_idx(&self) -> u64 {
        let mut rng = rand::thread_rng();
        loop {
            let idx = rng.gen();
            if !self.entries.contains_key(&idx) {
                return idx;
            }
        }
    }
}
