use rand::{self, Rng};
use std::collections::HashMap;
use value::{Value, Values};

#[derive(Default)]
pub struct Pool {
    values: HashMap<u64, Values>,
}

impl Pool {
    pub fn len(&self, idx: &u64) -> usize {
        match self.values.get(idx) {
            Some(&Values::Boolean(ref values)) => values.len(),
            Some(&Values::Int(ref values)) => values.len(),
            Some(&Values::String(ref values)) => values.len(),
            None => 0,
        }
    }

    pub fn is_column_materialized(&self, idx: &u64) -> bool {
        match self.values.get(idx) {
            Some(_) => true,
            None => false,
        }
    }

    pub fn get_values(&self, idx: &u64) -> Option<Values> {
        self.values.get(idx).cloned()
    }

    pub fn get_value(&self, col_idx: &u64, row_idx: &u64) -> Option<Value> {
        match self.values.get(col_idx) {
            Some(&Values::Boolean(ref values)) => {
                Self::get_clone(values.as_ref(), row_idx).map(Value::Boolean)
            }
            Some(&Values::Int(ref values)) => {
                Self::get_clone(values.as_ref(), row_idx).map(Value::Int)
            }
            Some(&Values::String(ref values)) => {
                Self::get_clone(values.as_ref(), row_idx).map(Value::String)
            }
            None => None,
        }
    }

    pub fn set_values(&mut self, idx: u64, values: Values) {
        self.values.insert(idx, values);
    }

    pub fn set_initial_values(&mut self, values: Values) -> u64 {
        let idx = self.unused_idx();
        self.values.insert(idx, values);
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
            if !self.values.contains_key(&idx) {
                return idx;
            }
        }
    }
}