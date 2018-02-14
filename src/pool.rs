use std::collections::HashMap;
use std::fmt;
use std::rc::Rc;
use std::sync::{Arc, Mutex};

use rand::{self, Rng};

use value::{ListValues, Value, Values};

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
    pub values: Rc<Values>,
    pub sorted: bool,
}

impl Entry {
    fn new(values: Rc<Values>, sorted: bool) -> Entry {
        Entry { values, sorted }
    }
}

macro_rules! get_value {
    ( $p:expr, $c:expr, $r:expr, $( $t:ident, $l:ident ),* ) => {
        match $p.entries.get($c).map(|entry| entry.values.as_ref()) {
            $(
                Some(&Values::$t(ref values)) => {
                    Pool::get_clone(values.as_ref(), $r).map(Value::$t)
                }
            )*
            Some(&Values::List(ref list_values)) => {
                match *list_values {
                    $(
                        ListValues::$t(ref values) => {
                            Pool::get_clone(values, $r).map(Value::$l)
                        }
                    )*
                }
            }
            None => None
        }
    };
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
        match self.entries.get(idx).map(|entry| entry.values.as_ref()) {
            Some(&Values::Boolean(ref values)) => values.len(),
            Some(&Values::Int(ref values)) => values.len(),
            Some(&Values::Float(ref values)) => values.len(),
            Some(&Values::String(ref values)) => values.len(),
            Some(&Values::List(ref values)) => values.len(),
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
        get_value!(
            self,
            col_idx,
            row_idx,
            Boolean,
            BooleanList,
            Int,
            IntList,
            Float,
            FloatList,
            String,
            StringList
        )
    }

    pub fn set_values(&mut self, idx: u64, values: Rc<Values>, sorted: bool) {
        self.entries.insert(idx, Entry::new(values, sorted));
    }

    pub fn set_initial_values(&mut self, values: Values) -> u64 {
        let idx = self.unused_idx();
        self.entries.insert(idx, Entry::new(Rc::new(values), false));
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
