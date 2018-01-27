use decorum::R64;
use pool::{self, Pool};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::result;
use value::{self, Predicate, Type, Value, Values};

#[derive(Debug)]
pub enum AggregateError {
    EmptyColumn,
    SumOnInvalidType(Type),
}

impl fmt::Display for AggregateError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            AggregateError::EmptyColumn => write!(f, "Aggregate on empty column"),
            AggregateError::SumOnInvalidType(ref type_) => {
                write!(f, "Sum aggregate on {:?} column", type_)
            }
        }
    }
}

type AggregateResult<T> = result::Result<T, AggregateError>;

#[derive(Debug)]
pub enum Error {
    MissingAggregates(Vec<String>),
    MissingColumnInIndices(String),
    SortingWithNoSortColumns,
    EmptyIndices,
    Aggregate(AggregateError),
    Pool(pool::Error),
    Value(value::Error),
}

impl From<AggregateError> for Error {
    fn from(error: AggregateError) -> Error {
        Error::Aggregate(error)
    }
}

impl From<pool::Error> for Error {
    fn from(error: pool::Error) -> Error {
        Error::Pool(error)
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
            Error::MissingAggregates(ref col_names) => {
                write!(f, "Missing aggregates for {:?}", col_names)
            }
            Error::MissingColumnInIndices(ref col_name) => {
                write!(f, "Missing column in indices {}", col_name)
            }
            Error::SortingWithNoSortColumns => write!(f, "Sort called without any sort columns"),
            Error::EmptyIndices => write!(f, "Empty pool indices"),
            Error::Aggregate(ref error) => write!(f, "{}", error),
            Error::Pool(ref error) => write!(f, "{}", error),
            Error::Value(ref error) => write!(f, "{}", error),
        }
    }
}

type Result<T> = result::Result<T, Error>;

#[derive(Debug)]
pub struct Row {
    values: Vec<Value>,
}

#[derive(Clone, Debug)]
struct Column {
    type_: Type,
}

impl Column {
    fn new(type_: Type) -> Column {
        Column { type_ }
    }
}

#[derive(Clone)]
pub struct Schema {
    columns: BTreeMap<String, Column>,
}

impl Schema {
    pub fn new(names: &[&str], types: &[Type]) -> Schema {
        let mut columns = BTreeMap::new();
        for (idx, name) in names.iter().enumerate() {
            columns.insert(name.to_string(), Column::new(types[idx].clone()));
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

macro_rules! simple_aggregate {
    ( $i:expr, $f:ident, $l:ident, $( $t:ident ),* ) => {
        match $i {
            Values::$l(_) => unimplemented!(),
            $(
                Values::$t(values) => Value::$t(Aggregator::$f(&values)?),
            )*
        }
    };
}
#[derive(Clone, Hash)]
pub enum Aggregator {
    First,
    Sum,
    Max,
    Min,
}

impl Aggregator {
    fn output_type(&self, input_type: &Type) -> AggregateResult<Type> {
        match *self {
            Aggregator::Sum => {
                if input_type == &Type::Int {
                    Ok(Type::Int)
                } else {
                    Err(AggregateError::SumOnInvalidType(input_type.clone()))
                }
            }
            Aggregator::First | Aggregator::Max | Aggregator::Min => Ok(input_type.clone()),
        }
    }

    fn aggregate(&self, input: Values) -> AggregateResult<Value> {
        Ok(match *self {
            Aggregator::First => {
                simple_aggregate!(input, first, List, Boolean, Int, Float, String)
            }
            Aggregator::Sum => {
                match input {
                    Values::Int(values) => Value::Int(values.iter().fold(0, |acc, &v| acc + v)),
                    Values::Float(values) => Value::Float(values.iter().fold(
                        R64::from_inner(0.0),
                        |acc, &v| acc + v,
                    )),
                    _ => return Err(AggregateError::SumOnInvalidType(input.type_())),
                }
            }
            Aggregator::Max => {
                simple_aggregate!(input, max, List, Boolean, Int, Float, String)
            }
            Aggregator::Min => {
                simple_aggregate!(input, min, List, Boolean, Int, Float, String)
            }
        })
    }

    fn first<T: Clone>(values: &[T]) -> AggregateResult<T> {
        match values.first() {
            Some(v) => Ok(v.clone()),
            None => Err(AggregateError::EmptyColumn),
        }
    }

    fn max<T: Clone + Ord>(values: &[T]) -> AggregateResult<T> {
        match values.iter().max() {
            Some(v) => Ok(v.clone()),
            None => Err(AggregateError::EmptyColumn),
        }
    }

    fn min<T: Clone + Ord>(values: &[T]) -> AggregateResult<T> {
        match values.iter().min() {
            Some(v) => Ok(v.clone()),
            None => Err(AggregateError::EmptyColumn),
        }
    }
}

#[derive(Clone, Hash)]
enum Operation {
    Select(Vec<String>),
    Filter(String, Predicate),
    Sort(Vec<String>),
    Aggregation(BTreeMap<String, Aggregator>),
    GroupBy(Vec<String>),
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
            pool_indices.insert(col_name, pool.set_initial_values(col_values));
        }
        DataFrame {
            schema,
            pool_indices,
            parent: None,
            operation: None,
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
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
        })
    }

    pub fn filter(&self, filter_column_name: &str, predicate: Predicate) -> Result<DataFrame> {
        let operation = Operation::Filter(filter_column_name.to_string(), predicate);
        Ok(DataFrame {
            pool_indices: Self::new_indices(&operation, &self.pool_indices),
            schema: self.schema.clone(),
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
        })
    }

    pub fn sort(&self, column_names: &[&str]) -> Result<DataFrame> {
        let operation = Operation::Sort(column_names.iter().map(|s| s.to_string()).collect());
        Ok(DataFrame {
            pool_indices: Self::new_indices(&operation, &self.pool_indices),
            schema: self.schema.clone(),
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
        })
    }

    pub fn aggregate(&self, aggregators: &BTreeMap<String, Aggregator>) -> Result<DataFrame> {
        {
            let missing: HashSet<&String> = &HashSet::from_iter(self.schema.columns.keys()) -
                &HashSet::from_iter(aggregators.keys());
            if !missing.is_empty() {
                return Err(Error::MissingAggregates(
                    missing.iter().map(|s| s.to_string()).collect(),
                ));
            }
        }
        let columns = self.schema
            .columns
            .iter()
            .map(|(name, column)| {
                let aggregator = &aggregators[name];
                Ok((
                    name.clone(),
                    Column::new(aggregator.output_type(&column.type_)?),
                ))
            })
            .collect::<Result<BTreeMap<String, Column>>>()?;
        let operation = Operation::Aggregation(aggregators.clone());
        Ok(DataFrame {
            pool_indices: Self::new_indices(&operation, &self.pool_indices),
            schema: Schema { columns },
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
        })
    }

    pub fn group_by(&self, column_names: &[&str]) -> Result<DataFrame> {
        let sorted = self.sort(column_names)?;
        let operation = Operation::GroupBy(column_names.iter().map(|s| s.to_string()).collect());
        let columns = sorted
            .schema
            .columns
            .iter()
            .map(|(name, column)| if column_names.contains(&name.as_str()) {
                (name.clone(), column.clone())
            } else {
                (
                    name.clone(),
                    Column::new(Type::List(Box::new(column.type_.clone()))),
                )
            })
            .collect::<BTreeMap<String, Column>>();
        Ok(DataFrame {
            pool_indices: Self::new_indices(&operation, &sorted.pool_indices),
            schema: Schema { columns },
            parent: Some(Box::new(sorted.clone())),
            operation: Some(operation),
        })
    }

    pub fn collect(&self, pool: &mut Pool) -> Result<Vec<Row>> {
        if self.should_materialize(pool) {
            self.materialize(pool)?;
        }

        let mut row_idx = 0;
        let mut rows = vec![];
        let result_size = pool.len(
            self.pool_indices.values().nth(0).ok_or(Error::EmptyIndices)?,
        ) as u64;
        
        loop {
            if row_idx == result_size {
                return Ok(rows);
            }
            let mut row_values = vec![];
            for (column_name, column) in &self.schema.columns {
                let col_idx = self.pool_indices[column_name];
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

    fn should_materialize(&self, pool: &Pool) -> bool {
        self.pool_indices.values().any(|idx| {
            !pool.is_column_materialized(idx)
        })
    }

    fn materialize(&self, pool: &mut Pool) -> Result<()> {
        let parent = match self.parent {
            Some(ref parent) => parent,
            None => return Ok(()),
        };
        let operation = match self.operation {
            Some(ref op) => op,
            None => unreachable!(),
        };

        if parent.should_materialize(pool) {
            parent.materialize(pool)?
        }

        match *operation {
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
                    filtered_values,
                    filter_entry.sorted,
                );

                for (col_name, idx) in &parent.pool_indices {
                    if col_name != filter_col_name {
                        let new_idx = operation.hash_from_seed(idx, col_name);
                        let entry = pool.get_entry(idx)?;
                        let values = entry.values.select_by_idx(&filter_pass_idxs);
                        pool.set_values(new_idx, values, entry.sorted)
                    }
                }
            }
            Operation::Sort(ref col_names) => {
                let mut parent_sorting: Option<Vec<usize>> = None;
                for col_name in col_names {
                    let col_idx = parent.get_idx(col_name)?;
                    let entry = pool.get_entry(&col_idx)?;
                    let (sort_indices, values) =
                        entry.values.sort(
                            &parent_sorting.as_ref().map(|s| s.as_slice()),
                            false,
                        );
                    pool.set_values(
                        operation.hash_from_seed(&col_idx, col_name),
                        values,
                        parent_sorting.is_none(),
                    );
                    parent_sorting = Some(sort_indices);
                }
                match parent_sorting {
                    Some(_) => {
                        let missing: HashSet<&String> =
                            &HashSet::from_iter(self.schema.columns.keys()) -
                                &HashSet::from_iter(col_names);
                        for col_name in missing {
                            let idx = parent.get_idx(col_name)?;
                            let entry = pool.get_entry(&idx)?;
                            let (_, values) =
                                entry.values.sort(
                                    &parent_sorting.as_ref().map(|s| s.as_slice()),
                                    true,
                                );
                            pool.set_values(
                                operation.hash_from_seed(&idx, col_name),
                                values,
                                false,
                            );
                        }
                    }
                    None => return Err(Error::SortingWithNoSortColumns),
                }
            }
            Operation::Aggregation(ref aggregators) => {
                for (col_name, idx) in &parent.pool_indices {
                    let aggregator = &aggregators[col_name];
                    let new_idx = operation.hash_from_seed(idx, col_name);
                    let entry = pool.get_entry(idx)?;
                    pool.set_values(
                        new_idx,
                        Values::from(aggregator.aggregate(entry.values)?),
                        true,
                    )
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
                    for group_col_values in &group_columns {
                        if !group_col_values.equal_at_idxs(row_idx, row_idx + 1) {
                            group_offsets.push(row_idx + 1);
                            continue;
                        }
                    }
                }

                for col_name in self.schema.columns.keys() {
                    let parent_idx = parent.get_idx(col_name)?;
                    let parent_entry = pool.get_entry(&parent_idx)?;
                    let idx = self.get_idx(col_name)?;
                    if col_names.contains(col_name) {
                        pool.set_values(idx, parent_entry.values.group_to_value(&group_offsets), false);
                    } else {
                        pool.set_values(idx, parent_entry.values.group_by(&group_offsets), false);
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

    fn new_indices(
        operation: &Operation,
        indices: &BTreeMap<String, u64>,
    ) -> BTreeMap<String, u64> {
        indices
            .iter()
            .map(|(col_name, idx)| {
                (col_name.clone(), operation.hash_from_seed(idx, col_name))
            })
            .collect()
    }
}
