use rand::{self, Rng};
use std::collections::HashMap;
use std::fmt;
use std::result;
use value::{Value, Values};

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

type Result<T> = result::Result<T, Error>;

#[derive(Clone, Debug)]
pub struct Entry {
    pub values: Values,
    pub sorted: bool,
}

impl Entry {
    fn new(values: Values, sorted: bool) -> Entry {
        Entry { values, sorted }
    }
}

#[derive(Default)]
pub struct Pool {
    entries: HashMap<u64, Entry>,
}

impl Pool {
    pub fn len(&self, idx: &u64) -> usize {
        match self.entries.get(idx).map(|entry| &entry.values) {
            Some(&Values::Boolean(ref values)) => values.len(),
            Some(&Values::Int(ref values)) => values.len(),
            Some(&Values::Float(ref values)) => values.len(),
            Some(&Values::String(ref values)) => values.len(),
            None => 0,
        }
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
        match self.entries.get(col_idx).map(|entry| &entry.values) {
            Some(&Values::Boolean(ref values)) => {
                Self::get_clone(values.as_ref(), row_idx).map(Value::Boolean)
            }
            Some(&Values::Int(ref values)) => {
                Self::get_clone(values.as_ref(), row_idx).map(Value::Int)
            }
            Some(&Values::Float(ref values)) => {
                Self::get_clone(values.as_ref(), row_idx).map(Value::Float)
            }
            Some(&Values::String(ref values)) => {
                Self::get_clone(values.as_ref(), row_idx).map(Value::String)
            }
            None => None,
        }
    }

    pub fn set_values(&mut self, idx: u64, values: Values, sorted: bool) {
        self.entries.insert(idx, Entry::new(values, sorted));
    }

    pub fn set_initial_values(&mut self, values: Values) -> u64 {
        let idx = self.unused_idx();
        self.entries.insert(idx, Entry::new(values, false));
        idx
    }

    fn get_clone<T>(slice: &[T], idx: &u64) -> Option<T>
    where
        T: Clone,
    {
        let idx = *idx as usize;
        if slice.len() > idx {
            Some(slice[idx].clone())
        } else {
            None
        }
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
