#![feature(plugin)]

#![plugin(clippy)]

extern crate rand;

use rand::Rng;
use std::collections::{BTreeMap, HashMap};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::rc::Rc;

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

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Boolean(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Value::Int(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

#[derive(Debug)]
pub struct Row {
    values: Vec<Value>,
}

#[derive(Clone, Debug)]
struct Column {
    type_: Type,
}

impl Column {
    fn new(type_: &Type) -> Column {
        Column { type_: type_.clone() }
    }
}

#[derive(Clone)]
pub struct Schema {
    columns: BTreeMap<String, Column>,
}

impl Schema {
    fn new(names: &[&str], types: &[Type]) -> Schema {
        let mut columns = BTreeMap::new();
        for (idx, name) in names.iter().enumerate() {
            columns.insert(name.to_string(), Column::new(&types[idx]));
        }
        Schema { columns }
    }

    fn select(&self, column_names: &[&str]) -> Schema {
        Schema {
            columns: self.columns
                .iter()
                .filter(|&(k, _)| column_names.contains(&k.as_str()))
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
    fn new(comparator: Comparator, value: Value) -> Predicate {
        Predicate { comparator, value }
    }

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

    #[allow(ptr_arg)]
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
    fn hash_from_seed(&self, seed: &u64, col_name: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        col_name.hash(&mut hasher);
        self.hash(&mut hasher);
        hasher.finish()
    }
}

#[derive(Clone, Debug)]
pub enum Values {
    Boolean(Rc<Vec<bool>>),
    Int(Rc<Vec<u64>>),
    String(Rc<Vec<String>>),
}

impl Values {
    fn filter(&self, predicate: &Predicate) -> (Vec<usize>, Values) {
        match *self {
            Values::Boolean(ref values) => {
                let filtered = values
                    .iter()
                    .enumerate()
                    .filter(|&(_, v)| predicate.boolean_pass(v))
                    .map(|(k, v)| (k, *v))
                    .collect::<Vec<(usize, bool)>>();
                (
                    filtered.iter().map(|&(k, _)| k).collect(),
                    Values::from(
                        filtered.into_iter().map(|(_, v)| v).collect::<Vec<bool>>(),
                    ),
                )
            }
            Values::Int(ref values) => {
                let filtered = values
                    .iter()
                    .enumerate()
                    .filter(|&(_, v)| predicate.int_pass(v))
                    .map(|(k, v)| (k, *v))
                    .collect::<Vec<(usize, u64)>>();
                (
                    filtered.iter().map(|&(k, _)| k).collect(),
                    Values::from(
                        filtered.into_iter().map(|(_, v)| v).collect::<Vec<u64>>(),
                    ),
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
                    Values::from(
                        filtered
                            .into_iter()
                            .map(|(_, v)| v)
                            .collect::<Vec<String>>(),
                    ),
                )
            }
        }
    }

    fn select_by_idx(&self, indices: &[usize]) -> Values {
        match *self {
            Values::Boolean(ref values) => {
                let filtered = values
                    .iter()
                    .enumerate()
                    .filter(|&(k, _)| indices.contains(&k))
                    .map(|(_, v)| *v)
                    .collect::<Vec<bool>>();
                Values::from(filtered)
            }
            Values::Int(ref values) => {
                let filtered = values
                    .iter()
                    .enumerate()
                    .filter(|&(k, _)| indices.contains(&k))
                    .map(|(_, v)| *v)
                    .collect::<Vec<u64>>();
                Values::from(filtered)
            }
            Values::String(ref values) => {
                let filtered = values
                    .iter()
                    .enumerate()
                    .filter(|&(k, _)| indices.contains(&k))
                    .map(|(_, v)| v.clone())
                    .collect::<Vec<String>>();
                Values::from(filtered)
            }
        }
    }
}

impl From<Vec<bool>> for Values {
    fn from(values: Vec<bool>) -> Self {
        Values::Boolean(Rc::new(values))
    }
}

impl From<Vec<u64>> for Values {
    fn from(values: Vec<u64>) -> Self {
        Values::Int(Rc::new(values))
    }
}

impl From<Vec<String>> for Values {
    fn from(values: Vec<String>) -> Self {
        Values::String(Rc::new(values))
    }
}

pub struct Pool {
    values: HashMap<u64, Values>,
}

impl Pool {
    fn new() -> Pool {
        Pool { values: HashMap::new() }
    }

    fn len(&self, idx: &u64) -> usize {
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

    fn set_values(&mut self, idx: u64, values: Values) {
        self.values.insert(idx, values);
    }

    fn set_initial_boolean_values(&mut self, values: Vec<bool>) -> u64 {
        let idx = self.unused_idx();
        self.values.insert(idx, Values::Boolean(Rc::from(values)));
        idx
    }

    fn set_initial_int_values(&mut self, values: Vec<u64>) -> u64 {
        let idx = self.unused_idx();
        self.values.insert(idx, Values::Int(Rc::from(values)));
        idx
    }

    fn set_initial_string_values(&mut self, values: Vec<String>) -> u64 {
        let idx = self.unused_idx();
        self.values.insert(idx, Values::String(Rc::from(values)));
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

#[derive(Clone)]
pub struct DataFrame {
    pub schema: Schema,
    parent: Option<Box<DataFrame>>,
    operation: Option<Operation>,
    pool_indices: BTreeMap<String, u64>,
}

impl DataFrame {
    pub fn new(pool: &mut Pool, schema: Schema, values: HashMap<String, Values>) -> DataFrame {
        let mut pool_indices = BTreeMap::new();
        for (col_name, col_values) in values {
            let idx = match col_values {
                Values::Boolean(v) => pool.set_initial_boolean_values(Self::take_or_clone(v)),
                Values::Int(v) => pool.set_initial_int_values(Self::take_or_clone(v)),
                Values::String(v) => pool.set_initial_string_values(Self::take_or_clone(v)),
            };
            pool_indices.insert(col_name, idx);
        }
        DataFrame {
            schema,
            pool_indices,
            parent: None,
            operation: None,
        }
    }

    pub fn select(&self, column_names: &[&str]) -> DataFrame {
        let operation = Operation::Select(column_names.iter().map(|s| s.to_string()).collect());
        let indices = self.pool_indices
            .iter()
            .filter(|&(k, _)| column_names.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), operation.hash_from_seed(v, k)))
            .collect();
        DataFrame {
            schema: self.schema.select(column_names),
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
            pool_indices: indices,
        }
    }

    pub fn filter(&self, filter_column_name: &str, predicate: Predicate) -> DataFrame {
        let operation = Operation::Filter(filter_column_name.to_string(), predicate);
        let indices = self.pool_indices
            .iter()
            .map(|(col_name, idx)| {
                (col_name.clone(), operation.hash_from_seed(idx, col_name))
            })
            .collect();
        DataFrame {
            schema: self.schema.clone(),
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
            pool_indices: indices,
        }
    }

    pub fn aggregate(&self, aggregators: HashMap<String, Aggregator>) -> DataFrame {
        unimplemented!()
    }

    pub fn collect(&self, pool: &mut Pool) -> Vec<Row> {
        if self.should_materialize(pool) {
            self.materialize(pool);
        }

        let mut row_idx = 0;
        let mut rows = vec![];
        let result_size = pool.len(self.pool_indices.values().nth(0).expect(
            "Empty pool_indices",
        )) as u64;

        loop {
            if row_idx == result_size {
                return rows;
            }
            let mut row_values = vec![];
            for (column_name, column) in &self.schema.columns {
                let col_idx = self.pool_indices[column_name];
                let value = match (&column.type_, pool.get_value(&col_idx, &row_idx)) {
                    (&Type::Boolean, Some(value @ Value::Boolean(_))) |
                    (&Type::Int, Some(value @ Value::Int(_))) |
                    (&Type::String, Some(value @ Value::String(_))) => value,
                    (_, None) => {
                        panic!(format!(
                            "Missing value: col => {:?}, row => {:?}",
                            col_idx,
                            row_idx
                        ))
                    }
                    (type_, value) => panic!(format!("Type error: {:?} != {:?}", type_, value)),
                };
                row_values.push(value)
            }
            rows.push(Row {
                values: row_values,
            });
            row_idx += 1;
        }
    }

    fn should_materialize(&self, pool: &Pool) -> bool {
        self.pool_indices.values().any(|idx| {
            !pool.is_column_materialized(idx)
        })
    }

    fn materialize(&self, pool: &mut Pool) {
        let parent = match self.parent {
            Some(ref parent) => parent,
            None => return,
        };
        let operation = match self.operation {
            Some(ref op) => op,
            None => unreachable!(),
        };

        if parent.should_materialize(pool) {
            parent.materialize(pool)
        }

        match *operation {
            Operation::Select(_) => {
                for (col_name, idx) in &self.pool_indices {
                    let parent_idx = parent.pool_indices.get(col_name).expect(&format!(
                        "Parent missing column: {}",
                        col_name
                    ));
                    let parent_values = pool.get_values(parent_idx).expect(&format!(
                        "Parent column missing in pool: {}",
                        parent_idx
                    ));
                    pool.set_values(*idx, parent_values)
                }
            }
            Operation::Filter(ref filter_col_name, ref predicate) => {
                let filter_col_idx = parent.pool_indices.get(filter_col_name).expect(&format!(
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
                    operation.hash_from_seed(filter_col_idx, filter_col_name),
                    filtered_values,
                );

                for (col_name, idx) in &parent.pool_indices {
                    if col_name != filter_col_name {
                        let new_idx = operation.hash_from_seed(idx, col_name);
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

    fn take_or_clone<T: Clone>(rc: Rc<T>) -> T {
        match Rc::try_unwrap(rc) {
            Ok(value) => value,
            Err(rc_value) => rc_value.as_ref().clone(),
        }
    }
}

fn main() {
    let mut pool = Pool::new();
    let schema = Schema::new(&["bool", "int"], &[Type::Boolean, Type::Int]);

    let mut values = HashMap::new();
    values.insert("bool".to_string(), Values::from(vec![true, false, true]));
    values.insert("int".to_string(), Values::from(vec![1, 2, 3]));

    let df = DataFrame::new(&mut pool, schema, values);
    let filter_df = df.filter("bool", Predicate::new(Comparator::Equal, Value::from(true)));
    let select_df = filter_df.select(&["int"]);

    println!("select_df.pool_indices: {:?}", select_df.pool_indices);
    println!("select_df.schema.columns: {:?}", select_df.schema.columns);
    println!("{:?}", select_df.collect(&mut pool));
}
