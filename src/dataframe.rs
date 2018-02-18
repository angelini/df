use std::collections::{BTreeMap, HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::rc::Rc;
use std::result;

use aggregate::{self, Aggregator};
use pool::{self, PoolRef};
use reader::{self, Format};
use schema::{Column, Schema};
use value::{self, Predicate, Type, Value, Values};

#[derive(Debug)]
pub enum Error {
    AggregatesOnGroupColumn(Vec<String>),
    MissingAggregates(Vec<String>),
    MissingColumnInIndices(String),
    OrderByWithNoSortColumns,
    EmptyIndices,
    Aggregate(aggregate::Error),
    Pool(pool::Error),
    Reader(reader::Error),
    Value(value::Error),
}

impl From<aggregate::Error> for Error {
    fn from(error: aggregate::Error) -> Error {
        Error::Aggregate(error)
    }
}

impl From<pool::Error> for Error {
    fn from(error: pool::Error) -> Error {
        Error::Pool(error)
    }
}

impl From<reader::Error> for Error {
    fn from(error: reader::Error) -> Error {
        Error::Reader(error)
    }
}

impl From<value::Error> for Error {
    fn from(error: value::Error) -> Error {
        Error::Value(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::AggregatesOnGroupColumn(ref col_names) => {
                write!(f, "Aggregates on group columns {:?}", col_names)
            }
            Error::MissingAggregates(ref col_names) => {
                write!(f, "Missing aggregates for {:?}", col_names)
            }
            Error::MissingColumnInIndices(ref col_name) => {
                write!(f, "Missing column in indices {}", col_name)
            }
            Error::OrderByWithNoSortColumns => {
                write!(f, "Order by called without any sort columns")
            }
            Error::EmptyIndices => write!(f, "Empty pool indices"),
            Error::Aggregate(ref error) => write!(f, "{}", error),
            Error::Pool(ref error) => write!(f, "{}", error),
            Error::Reader(ref error) => write!(f, "{}", error),
            Error::Value(ref error) => write!(f, "{}", error),
        }
    }
}

pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, Eq, PartialEq)]
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Row {
        Row { values }
    }
}

#[derive(Clone, Debug, Deserialize, Hash, Serialize)]
pub enum Operation {
    Read(Format, PathBuf),
    Select(Vec<String>),
    Filter(String, Predicate),
    OrderBy(Vec<String>),
    GroupBy(Vec<String>),
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

fn as_strs(strings: &[String]) -> Vec<&str> {
    strings.iter().map(|s| s.as_str()).collect()
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct DataFrame {
    pub schema: Schema,
    parent: Option<Box<DataFrame>>,
    operation: Option<Operation>,
    grouped_by: Vec<String>,
    ordered_by: Vec<String>,
    pool_indices: HashMap<String, u64>,
}

impl DataFrame {
    pub fn new(pool: &PoolRef, schema: Schema, values: HashMap<String, Values>) -> DataFrame {
        let mut pool_indices = HashMap::new();
        for (col_name, col_values) in values {
            pool_indices.insert(
                col_name,
                pool.lock().unwrap().set_initial_values(col_values),
            );
        }
        DataFrame {
            schema,
            pool_indices,
            parent: None,
            operation: None,
            ordered_by: vec![],
            grouped_by: vec![],
        }
    }

    pub fn read(format: &Format, path: &Path, schema: &Schema) -> DataFrame {
        DataFrame {
            schema: schema.clone(),
            parent: None,
            operation: Some(Operation::Read(format.clone(), path.to_path_buf())),
            ordered_by: vec![],
            grouped_by: vec![],
            pool_indices: format.indices(path, schema),
        }
    }

    pub fn select(&self, column_names: &[&str]) -> Result<DataFrame> {
        let operation = Operation::Select(column_names.iter().map(|s| s.to_string()).collect());
        let pool_indices = self.pool_indices
            .iter()
            .filter(|&(k, _)| column_names.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), operation.hash_from_seed(v, k)))
            .collect();
        Ok(DataFrame {
            pool_indices,
            schema: self.schema.select(column_names),
            parent: Some(box self.clone()),
            operation: Some(operation),
            ordered_by: self.ordered_by.clone(),
            grouped_by: self.grouped_by.clone(),
        })
    }

    pub fn filter(&self, filter_column_name: &str, predicate: &Predicate) -> Result<DataFrame> {
        let operation = Operation::Filter(filter_column_name.to_string(), predicate.clone());
        Ok(DataFrame {
            pool_indices: Self::new_indices(&operation, &self.pool_indices),
            schema: self.schema.clone(),
            parent: Some(box self.clone()),
            operation: Some(operation),
            ordered_by: self.ordered_by.clone(),
            grouped_by: self.grouped_by.clone(),
        })
    }

    pub fn order_by(&self, column_names: &[&str]) -> Result<DataFrame> {
        if self.ordered_by == column_names {
            return Ok(self.clone());
        }
        let col_name_strings = column_names
            .iter()
            .map(|s| s.to_string())
            .collect::<Vec<String>>();
        let operation = Operation::OrderBy(col_name_strings.clone());
        Ok(DataFrame {
            pool_indices: Self::new_indices(&operation, &self.pool_indices),
            schema: self.schema.clone(),
            parent: Some(box self.clone()),
            operation: Some(operation),
            ordered_by: col_name_strings,
            grouped_by: self.grouped_by.clone(),
        })
    }

    pub fn group_by(&self, column_names: &[&str]) -> Result<DataFrame> {
        if self.grouped_by == column_names {
            return Ok(self.clone());
        }
        let ordered = if self.ordered_by == column_names {
            self.clone()
        } else {
            self.order_by(column_names)?
        };
        let operation = Operation::GroupBy(
            column_names
                .iter()
                .map(|s| s.to_string())
                .collect::<Vec<String>>(),
        );
        let columns = ordered
            .schema
            .iter()
            .map(|column| if column_names.contains(&column.name.as_str()) {
                column.clone()
            } else {
                Column::new(column.name.clone(), Type::List(box column.type_.clone()))
            })
            .collect::<Vec<Column>>();
        Ok(DataFrame {
            pool_indices: Self::new_indices(&operation, &ordered.pool_indices),
            schema: Schema { columns },
            parent: Some(box ordered.clone()),
            operation: Some(operation),
            ordered_by: ordered.ordered_by.clone(),
            grouped_by: ordered.ordered_by.clone(),
        })
    }

    pub fn aggregate(&self, aggregators: &BTreeMap<String, Aggregator>) -> Result<DataFrame> {
        {
            let aggregate_keys: HashSet<&String> = HashSet::from_iter(aggregators.keys());
            let group_keys = HashSet::from_iter(&self.grouped_by);
            let overlap = &aggregate_keys & &group_keys;
            if !overlap.is_empty() {
                return Err(Error::AggregatesOnGroupColumn(
                    overlap.iter().map(|s| s.to_string()).collect(),
                ));
            }
            let missing = &(&HashSet::from_iter(self.schema.keys()) - &aggregate_keys) -
                &group_keys;
            if !missing.is_empty() {
                return Err(Error::MissingAggregates(
                    missing.iter().map(|s| s.to_string()).collect(),
                ));
            }
        }
        let columns = self.schema
            .iter()
            .map(|column| if aggregators.contains_key(&column.name) {
                let aggregator = &aggregators[&column.name];
                Ok(Column::new(
                    column.name.clone(),
                    aggregator.output_type(&column.type_)?,
                ))
            } else {
                Ok(column.clone())
            })
            .collect::<Result<Vec<Column>>>()?;
        let operation = Operation::Aggregation(aggregators.clone());
        Ok(DataFrame {
            pool_indices: Self::new_indices(&operation, &self.pool_indices),
            schema: Schema { columns },
            parent: Some(box self.clone()),
            operation: Some(operation),
            ordered_by: self.ordered_by.clone(),
            grouped_by: self.grouped_by.clone(),
        })
    }

    pub fn call(&self, operation: &Operation) -> Result<DataFrame> {
        Ok(match *operation {
            Operation::Read(_, _) => unimplemented!(),
            Operation::Select(ref column_names) => self.select(&as_strs(column_names))?,
            Operation::Filter(ref column_name, ref predicate) => {
                self.filter(column_name, predicate)?
            }
            Operation::OrderBy(ref column_names) => self.order_by(&as_strs(column_names))?,
            Operation::GroupBy(ref column_names) => self.group_by(&as_strs(column_names))?,
            Operation::Aggregation(ref aggregators) => self.aggregate(aggregators)?,
        })
    }

    pub fn collect(&self, pool: &PoolRef) -> Result<Vec<Row>> {
        if self.should_materialize(pool) {
            self.materialize(pool)?;
        }

        let mut row_idx = 0;
        let mut rows = vec![];
        let pool = pool.lock().unwrap();
        let result_size = pool.len(
            self.pool_indices.values().nth(0).ok_or(Error::EmptyIndices)?,
        ) as u64;

        loop {
            if row_idx == result_size {
                return Ok(rows);
            }
            let mut row_values = vec![];
            for column in self.schema.iter() {
                let col_idx = self.pool_indices[&column.name];
                let value = match (&column.type_, pool.get_value(&col_idx, &row_idx)) {
                    (&Type::Boolean, Some(value @ Value::Boolean(_))) |
                    (&Type::Int, Some(value @ Value::Int(_))) |
                    (&Type::Float, Some(value @ Value::Float(_))) |
                    (&Type::String, Some(value @ Value::String(_))) |
                    (&Type::List(box Type::Boolean), Some(value @ Value::BooleanList(_))) |
                    (&Type::List(box Type::Int), Some(value @ Value::IntList(_))) |
                    (&Type::List(box Type::Float), Some(value @ Value::FloatList(_))) |
                    (&Type::List(box Type::String), Some(value @ Value::StringList(_))) => value,
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
            rows.push(Row { values: row_values });
            row_idx += 1;
        }
    }

    pub fn as_values(&self, pool: &PoolRef) -> Result<HashMap<String, Values>> {
        if self.should_materialize(pool) {
            self.materialize(pool)?;
        }

        let mut results = HashMap::new();
        for (name, idx) in &self.pool_indices {
            results.insert(
                name.clone(),
                pool.lock().unwrap().get_entry(idx)?.values.as_ref().clone(),
            );
        }
        Ok(results)
    }

    pub fn print_indices(&self, name: &str) {
        println!("{} -> {:?}", name, self.pool_indices)
    }

    fn should_materialize(&self, pool: &PoolRef) -> bool {
        self.pool_indices.values().any(|idx| {
            !pool.lock().unwrap().is_column_materialized(idx)
        })
    }

    fn materialize(&self, pool: &PoolRef) -> Result<()> {
        let operation = match self.operation {
            Some(ref op) => op,
            None => unreachable!(),
        };

        if let Operation::Read(ref format, ref path) = *operation {
            let mut values = format.read(path, &self.schema)?;
            for (col_name, idx) in &self.pool_indices {
                pool.lock().unwrap().set_values(
                    *idx,
                    Rc::new(
                        values.remove(col_name).unwrap(),
                    ),
                    false,
                )
            }
            return Ok(());
        }

        let parent = match self.parent {
            Some(ref parent) => parent,
            None => return Ok(()),
        };
        if parent.should_materialize(pool) {
            parent.materialize(pool)?
        }
        let mut pool = pool.lock().unwrap();

        match *operation {
            Operation::Read(ref format, ref path) => {
                println!("format: {:?}", format);
                let mut values = format.read(path, &self.schema)?;
                for (col_name, idx) in &self.pool_indices {
                    pool.set_values(*idx, Rc::new(values.remove(col_name).unwrap()), false)
                }
            }
            Operation::Select(_) => {
                for (col_name, idx) in &self.pool_indices {
                    let parent_idx = parent.get_idx(col_name)?;
                    let parent_entry = pool.get_entry(&parent_idx)?;
                    pool.set_values(*idx, parent_entry.values, parent_entry.sorted)
                }
            }
            Operation::Filter(ref filter_col_name, ref predicate) => {
                let filter_col_idx = parent.get_idx(filter_col_name)?;
                let filter_entry = pool.get_entry(&filter_col_idx)?;
                let (filter_pass_idxs, filtered_values) = predicate.filter(&filter_entry.values)?;
                pool.set_values(
                    operation.hash_from_seed(&filter_col_idx, filter_col_name),
                    Rc::new(filtered_values),
                    filter_entry.sorted,
                );

                for (col_name, idx) in &parent.pool_indices {
                    if col_name != filter_col_name {
                        let new_idx = operation.hash_from_seed(idx, col_name);
                        let entry = pool.get_entry(idx)?;
                        let values = entry.values.select_by_idx(&filter_pass_idxs);
                        pool.set_values(new_idx, Rc::new(values), entry.sorted)
                    }
                }
            }
            Operation::OrderBy(ref col_names) => {
                let mut sort_scores: Option<HashMap<usize, usize>> = None;
                for col_name in col_names {
                    let col_idx = parent.get_idx(col_name)?;
                    let entry = pool.get_entry(&col_idx)?;
                    let (sort_indices, values) = entry.values.order_by(&sort_scores, false);
                    pool.set_values(
                        operation.hash_from_seed(&col_idx, col_name),
                        Rc::new(values),
                        sort_scores.is_none(),
                    );
                    sort_scores = Some(sort_indices);
                }
                match sort_scores {
                    Some(_) => {
                        let missing: HashSet<&String> = &HashSet::from_iter(self.schema.keys()) -
                            &HashSet::from_iter(col_names);
                        for col_name in missing {
                            let idx = parent.get_idx(col_name)?;
                            let entry = pool.get_entry(&idx)?;
                            let (_, values) = entry.values.order_by(&sort_scores, true);
                            pool.set_values(
                                operation.hash_from_seed(&idx, col_name),
                                Rc::new(values),
                                false,
                            );
                        }
                    }
                    None => return Err(Error::OrderByWithNoSortColumns),
                }
            }
            Operation::GroupBy(ref col_names) => {
                let mut len = 0;
                let mut group_columns = vec![];
                for col_name in col_names {
                    let parent_idx = parent.get_idx(col_name)?;
                    let parent_entry = pool.get_entry(&parent_idx)?;
                    len = parent_entry.values.len();
                    group_columns.push(parent_entry.values);
                }

                let mut group_offsets = vec![];
                for row_idx in 0..(len - 1) {
                    let next_is_different = group_columns
                        .iter()
                        .map(|vals| vals.equal_at_idxs(row_idx, row_idx + 1))
                        .any(|is_equal| !is_equal);
                    if next_is_different {
                        group_offsets.push(row_idx);
                    }
                }
                group_offsets.push(len - 1);

                for col_name in self.schema.keys() {
                    let parent_idx = parent.get_idx(col_name)?;
                    let parent_entry = pool.get_entry(&parent_idx)?;
                    let idx = self.get_idx(col_name)?;
                    if col_names.contains(col_name) {
                        pool.set_values(
                            idx,
                            Rc::new(parent_entry.values.keep(&group_offsets)),
                            parent_entry.sorted,
                        );
                    } else {
                        pool.set_values(
                            idx,
                            Rc::new(parent_entry.values.group_by(&group_offsets)),
                            false,
                        );
                    }
                }
            }
            Operation::Aggregation(ref aggregators) => {
                for (col_name, idx) in &parent.pool_indices {
                    let new_idx = operation.hash_from_seed(idx, col_name);
                    let entry = pool.get_entry(idx)?;
                    if aggregators.contains_key(col_name) {
                        let aggregator = &aggregators[col_name];
                        pool.set_values(
                            new_idx,
                            Rc::new(aggregator.aggregate(&entry.values)?),
                            false,
                        )
                    } else {
                        pool.set_values(new_idx, entry.values, entry.sorted)
                    }
                }
            }
        }
        Ok(())
    }

    fn get_idx(&self, col_name: &str) -> Result<u64> {
        self.pool_indices
            .get(col_name)
            .ok_or_else(|| Error::MissingColumnInIndices(col_name.to_string()))
            .map(|idx| *idx)
    }

    fn new_indices(operation: &Operation, indices: &HashMap<String, u64>) -> HashMap<String, u64> {
        indices
            .iter()
            .map(|(col_name, idx)| {
                (col_name.clone(), operation.hash_from_seed(idx, col_name))
            })
            .collect()
    }
}
