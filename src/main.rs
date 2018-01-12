extern crate rand;

use rand::Rng;
use std::borrow::{Borrow, Cow};
use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone, Debug)]
pub enum Type {
    Boolean,
    Int,
    String,
}

#[derive(Clone, Debug, Hash)]
pub enum Value {
    Boolean(bool),
    Int(u64),
    String(String),
}

pub struct Row {
    values: Vec<Value>,
    types: Vec<Type>, // FIXME: Don't include on every row
}

#[derive(Clone)]
struct Column {
    type_: Type,
}

#[derive(Clone)]
pub struct Schema {
    columns: BTreeMap<String, Column>,
}

impl Schema {
    fn select(&self, column_names: &[String]) -> Schema {
        Schema {
            columns: self.columns
                .iter()
                .filter(|&(k, _)| column_names.contains(&k))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

#[derive(Clone, Hash)]
pub enum Aggregator {
    First,
    Sum,
    Max,
    Min,
}

#[derive(Clone, Hash)]
pub enum Comparator {
    Equal,
    GreaterThan,
    GreaterThanOrEq,
    LessThan,
    LessThanOrEq,
}

impl Comparator {
    fn pass<T: Eq + Ord>(&self, left: &T, right: &T) -> bool {
        match *self {
            Comparator::Equal => left == right,
            Comparator::GreaterThan => left > right,
            Comparator::GreaterThanOrEq => left >= right,
            Comparator::LessThan => left < right,
            Comparator::LessThanOrEq => left <= right,
        }
    }
}

#[derive(Clone, Hash)]
pub struct Predicate {
    comparator: Comparator,
    value: Value,
}

impl Predicate {
    fn boolean_pass(&self, other: &bool) -> bool {
        match self.value {
            Value::Boolean(value) => self.comparator.pass(&value, other),
            _ => panic!(format!("Type error: boolean predicate against {:?}", other)),
        }
    }

    fn int_pass(&self, other: &u64) -> bool {
        match self.value {
            Value::Int(value) => self.comparator.pass(&value, other),
            _ => panic!(format!("Type error: boolean predicate against {:?}", other)),
        }
    }

    fn string_pass(&self, other: &String) -> bool {
        match self.value {
            Value::String(ref value) => self.comparator.pass(value, other),
            _ => panic!(format!("Type error: boolean predicate against {:?}", other)),
        }
    }
}

#[derive(Clone, Hash)]
enum Operation {
    Select(Vec<String>),
    Filter(String, Predicate),
    Aggregation(BTreeMap<String, Aggregator>),
}

impl Operation {
    fn hash_from_seed(&self, seed: &u64) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Clone)]
enum Values<'a> {
    Boolean(Cow<'a, [bool]>),
    Int(Cow<'a, [u64]>),
    String(Cow<'a, [String]>),
}

impl<'a> Values<'a> {
    fn filter<'b>(&self, predicate: &Predicate) -> (Vec<usize>, Values<'b>) {
        match *self {
            Values::Boolean(ref values) => {
                let filtered = values
                    .iter()
                    .enumerate()
                    .filter(|&(_, v)| predicate.boolean_pass(v))
                    .map(|(k, v)| (k, v.clone()))
                    .collect::<Vec<(usize, bool)>>();
                (
                    filtered.iter().map(|&(k, _)| k).collect(),
                    Values::Boolean(Cow::from(
                        filtered.into_iter().map(|(_, v)| v).collect::<Vec<bool>>(),
                    )),
                )
            }
            Values::Int(ref values) => {
                let filtered = values
                    .iter()
                    .enumerate()
                    .filter(|&(_, v)| predicate.int_pass(v))
                    .map(|(k, v)| (k, v.clone()))
                    .collect::<Vec<(usize, u64)>>();
                (
                    filtered.iter().map(|&(k, _)| k).collect(),
                    Values::Int(Cow::from(
                        filtered.into_iter().map(|(_, v)| v).collect::<Vec<u64>>(),
                    )),
                )
            }
            Values::String(ref values) => {
                let filtered = values
                    .iter()
                    .enumerate()
                    .filter(|&(_, v)| predicate.string_pass(v))
                    .map(|(k, v)| (k, v.clone()))
                    .collect::<Vec<(usize, String)>>();
                (
                    filtered.iter().map(|&(k, _)| k).collect(),
                    Values::String(Cow::from(
                        filtered
                            .into_iter()
                            .map(|(_, v)| v)
                            .collect::<Vec<String>>(),
                    )),
                )
            }
        }
    }

    fn select_by_idx<'b>(&self, indices: &[usize]) -> Values<'b> {
        match *self {
            Values::Boolean(ref values) => {
                let filtered = values
                    .iter()
                    .enumerate()
                    .filter(|&(k, _)| indices.contains(&k))
                    .map(|(k, v)| v.clone())
                    .collect::<Vec<bool>>();
                Values::Boolean(Cow::from(filtered))
            }
            _ => unimplemented!(),
        }
    }
}

pub struct Pool<'a> {
    values: HashMap<u64, Values<'a>>,
}

impl<'a> Pool<'a> {
    fn size(&self, idx: &u64) -> usize {
        match self.values.get(idx) {
            Some(&Values::Boolean(ref values)) => values.len(),
            Some(&Values::Int(ref values)) => values.len(),
            Some(&Values::String(ref values)) => values.len(),
            None => 0,
        }
    }

    fn is_column_materialized(&self, idx: &u64) -> bool {
        match self.values.get(idx) {
            Some(_) => true,
            None => false,
        }
    }

    fn get_values(&self, idx: &u64) -> Option<Values> {
        self.values.get(idx).cloned()
    }

    fn get_value(&self, col_idx: &u64, row_idx: &u64) -> Option<Value> {
        match self.values.get(col_idx) {
            Some(&Values::Boolean(ref values)) => {
                Self::get_clone(values.borrow(), row_idx).map(|v| Value::Boolean(v))
            }
            Some(&Values::Int(ref values)) => {
                Self::get_clone(values.borrow(), row_idx).map(|v| Value::Int(v))
            }
            Some(&Values::String(ref values)) => {
                Self::get_clone(values.borrow(), row_idx).map(|v| Value::String(v))
            }
            None => None,
        }
    }

    fn set_values(&mut self, idx: u64, values: Values<'a>) {
        self.values.insert(idx, values);
    }

    fn set_initial_boolean_values(&mut self, values: Vec<bool>) -> u64 {
        let idx = self.unused_idx();
        self.values.insert(idx, Values::Boolean(Cow::from(values)));
        idx
    }

    fn set_initial_int_values(&mut self, values: Vec<u64>) -> u64 {
        let idx = self.unused_idx();
        self.values.insert(idx, Values::Int(Cow::from(values)));
        idx
    }

    fn set_initial_string_values(&mut self, values: Vec<String>) -> u64 {
        let idx = self.unused_idx();
        self.values.insert(idx, Values::String(Cow::from(values)));
        idx
    }

    fn get_clone<T>(slice: &[T], idx: &u64) -> Option<T>
    where
        T: Clone,
    {
        let idx = *idx as usize;
        if slice.len() >= idx {
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

#[derive(Clone)]
pub struct DataFrame {
    pub schema: Schema,
    parent: Box<DataFrame>,
    operation: Operation,
    pool_indices: BTreeMap<String, u64>,
}

impl DataFrame {
    pub fn select(&self, column_names: Vec<String>) -> DataFrame {
        let indices = self.pool_indices
            .iter()
            .filter(|&(k, _)| column_names.contains(k))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        DataFrame {
            schema: self.schema.select(&column_names),
            parent: Box::new(self.clone()),
            operation: Operation::Select(column_names),
            pool_indices: indices,
        }
    }

    pub fn filter(&self, column_name: String, predicate: Predicate) -> DataFrame {
        let seed = self.pool_indices[&column_name];
        let operation = Operation::Filter(column_name.clone(), predicate);
        let indices = self.pool_indices
            .keys()
            .map(|k| (k.clone(), operation.hash_from_seed(&seed)))
            .collect();
        DataFrame {
            schema: self.schema.clone(),
            parent: Box::new(self.clone()),
            operation: operation,
            pool_indices: indices,
        }
    }

    pub fn aggregate(&self, aggregators: HashMap<String, Aggregator>) -> DataFrame {
        unimplemented!()
    }

    pub fn collect(&self, pool: &mut Pool) -> Vec<Row> {
        let types = self.schema
            .columns
            .iter()
            .map(|(_, c)| c.type_.clone())
            .collect::<Vec<Type>>();
        let mut row_idx = 0;
        let mut rows = vec![];
        let result_size = *self.pool_indices.values().nth(0).unwrap();

        for (column_name, column) in &self.schema.columns {
            let col_idx = self.pool_indices[column_name];
            if !pool.is_column_materialized(&col_idx) {}
        }

        loop {
            if row_idx == result_size {
                return rows;
            }
            let mut row_values = vec![];
            for (column_name, column) in &self.schema.columns {
                let col_idx = self.pool_indices[column_name];
                let value = match (&column.type_, pool.get_value(&col_idx, &row_idx)) {
                    (&Type::Boolean, Some(value @ Value::Boolean(_))) => value,
                    (&Type::Int, Some(value @ Value::Int(_))) => value,
                    (&Type::String, Some(value @ Value::String(_))) => value,
                    (_, None) => {
                        panic!(format!(
                            "Missing value: col => {:?}, row => {:?}",
                            col_idx,
                            row_idx
                        ))
                    }
                    (type_ @ _, value @ _) => {
                        panic!(format!("Type error: {:?} != {:?}", type_, value))
                    }
                };
                row_values.push(value)
            }
            rows.push(Row {
                values: row_values,
                types: types.clone(),
            });
            row_idx += 1;
        }
    }

    fn materialize<'a>(&self, pool: &'a mut Pool<'a>) {
        match self.operation {
            Operation::Select(_) => {}
            Operation::Filter(ref filter_col_name, ref predicate) => {
                let filter_col_idx = self.pool_indices.get(filter_col_name).expect(&format!(
                    "Column missing in pool_indices: filter col => {}",
                    filter_col_name
                ));

                let (filter_pass_idxs, filtered_values) = pool.get_values(filter_col_idx)
                    .expect(&format!(
                        "Column missing in pool: filter col idx {}",
                        filter_col_idx
                    ))
                    .filter(predicate);
                pool.set_values(
                    self.operation.hash_from_seed(filter_col_idx),
                    filtered_values,
                );

                for (col_name, idx) in &self.pool_indices {
                    if col_name != filter_col_name {
                        let new_idx = self.operation.hash_from_seed(idx);
                        let values = pool.get_values(idx)
                            .expect(&format!("Column missing in pool: {}", col_name))
                            .select_by_idx(&filter_pass_idxs);
                        pool.set_values(new_idx, values)
                    }
                }
            }
            Operation::Aggregation(_) => unimplemented!(),
        }
    }
}

fn main() {
    println!("Hello world");
}
