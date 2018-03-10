use std::cmp;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use std::path::{Path, PathBuf};
use std::result;
use std::sync::Arc;

use futures::future::{self, Future};
use futures_cpupool::CpuPool;

use aggregate::{self, Aggregator};
use block::{self, AnyBlock, ArithmeticOp, Block};
use pool::{self, Entry, Pool, PoolRef};
use reader::{self, Format};
use schema::{Column, Schema};
use value::{Predicate, Type, Value};

#[derive(Debug)]
pub enum Error {
    AggregatesOnGroupColumn(Vec<String>),
    AliasRequired,
    EmptyIndices,
    MissingAggregates(Vec<String>),
    MissingColumnInIndices(String),
    MissingColumnInSchema(Schema, String),
    OrderByWithNoSortColumns,
    WithoutSource(String),
    Aggregate(aggregate::Error),
    Block(block::Error),
    Pool(pool::Error),
    Reader(reader::Error),
}

impl From<aggregate::Error> for Error {
    fn from(error: aggregate::Error) -> Error {
        Error::Aggregate(error)
    }
}

impl From<block::Error> for Error {
    fn from(error: block::Error) -> Error {
        Error::Block(error)
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

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::AggregatesOnGroupColumn(ref col_names) => {
                write!(f, "Aggregates on group columns {:?}", col_names)
            }
            Error::AliasRequired => write!(f, "Alias required for column expressions"),
            Error::EmptyIndices => write!(f, "Empty pool indices"),
            Error::MissingAggregates(ref col_names) => {
                write!(f, "Missing aggregates for {:?}", col_names)
            }
            Error::MissingColumnInIndices(ref col_name) => {
                write!(f, "Missing column in indices {}", col_name)
            }
            Error::MissingColumnInSchema(ref schema, ref col_name) => {
                write!(f, "Missing column {} in schema {:?}", col_name, schema)
            }
            Error::OrderByWithNoSortColumns => {
                write!(f, "Order by called without any sort columns")
            }
            Error::WithoutSource(ref alias) => {
                write!(f, "Aliased column expression {} has no source", alias)
            }
            Error::Aggregate(ref error) => write!(f, "{}", error),
            Error::Block(ref error) => write!(f, "{}", error),
            Error::Pool(ref error) => write!(f, "{}", error),
            Error::Reader(ref error) => write!(f, "{}", error),
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
pub enum ColumnExpr {
    Constant(Value),
    Source(String),
    Alias(String, Box<ColumnExpr>),
    Operation(ArithmeticOp, Box<ColumnExpr>, Box<ColumnExpr>),
}

impl ColumnExpr {
    fn name(&self) -> Option<String> {
        match *self {
            ColumnExpr::Source(ref col_name) |
            ColumnExpr::Alias(ref col_name, _) => Some(col_name.clone()),
            _ => None,
        }
    }

    fn source_name(&self) -> Option<String> {
        match *self {
            ColumnExpr::Constant(_) => None,
            ColumnExpr::Source(ref name) => Some(name.clone()),
            ColumnExpr::Alias(_, ref child_expr) => child_expr.source_name(),
            ColumnExpr::Operation(_, ref left, ref right) => {
                let mut source = left.source_name();
                if source.is_none() {
                    source = right.source_name()
                }
                source
            }
        }
    }

    fn type_(&self, parent_schema: &Schema) -> Result<Type> {
        match *self {
            ColumnExpr::Constant(ref value) => Ok(value.type_()),
            ColumnExpr::Source(ref col_name) => {
                parent_schema.type_(col_name).ok_or_else(|| {
                    Error::MissingColumnInSchema(parent_schema.clone(), col_name.clone())
                })
            }
            ColumnExpr::Alias(_, ref expr) => expr.type_(parent_schema),
            ColumnExpr::Operation(ref operation, ref left, ref right) => {
                Ok(operation.type_(
                    &left.type_(parent_schema)?,
                    &right.type_(parent_schema)?,
                )?)
            }
        }
    }

    fn source_names(column_exprs: &[&ColumnExpr]) -> Vec<String> {
        column_exprs
            .into_iter()
            .flat_map(|expr| match **expr {
                ColumnExpr::Source(ref col_name) => vec![col_name.to_string()],
                ColumnExpr::Alias(_, ref child) => ColumnExpr::source_names(&[child]),
                ColumnExpr::Operation(_, ref left, ref right) => {
                    let mut left_names = ColumnExpr::source_names(&[left]);
                    let mut right_names = ColumnExpr::source_names(&[right]);
                    left_names.append(&mut right_names);
                    left_names
                }
                _ => vec![],
            })
            .collect()
    }
}

#[derive(Clone, Debug, Deserialize, Hash, Serialize)]
pub enum Operation {
    Read(Format, PathBuf),
    Select(Vec<ColumnExpr>),
    Filter(String, Predicate),
    OrderBy(Vec<String>),
    GroupBy(Vec<String>),
    Aggregation(BTreeMap<String, Aggregator>),
}

impl Operation {
    fn hash_from_seed(&self, seed: &u64, col_name: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        seed.hash(&mut hasher);
        if let Operation::Select(ref column_exprs) = *self {
            let named = column_exprs
                .into_iter()
                .map(|expr| (expr.name().unwrap(), expr))
                .collect::<HashMap<String, &ColumnExpr>>();
            named[col_name].hash(&mut hasher);
        }
        self.hash(&mut hasher);
        hasher.finish()
    }
}

fn as_strs(strings: &[String]) -> Vec<&str> {
    strings.iter().map(|s| s.as_str()).collect()
}

fn order_by_future(
    idx: u64,
    block: Arc<Block>,
    sort_order: Arc<Vec<usize>>,
) -> Box<Future<Item = (u64, Box<Block>), Error = ()> + Send> {
    assert_eq!(block.len(), sort_order.len());
    Box::new(future::lazy(move || {
        future::ok((idx, block.order_by(sort_order.as_ref())))
    }))
}

type FilteredBlock = (u64, bool, Box<Block>);

fn filter_future(
    idx: u64,
    block: Arc<Block>,
    pass_indices: Arc<Vec<usize>>,
    sorted: bool,
) -> Box<Future<Item = FilteredBlock, Error = ()> + Send> {
    Box::new(future::lazy(move || {
        future::ok((idx, sorted, block.select_by_idx(pass_indices.as_ref())))
    }))
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
    pub fn new(pool: &PoolRef, schema: Schema, blocks: HashMap<String, Box<Block>>) -> DataFrame {
        let mut pool_indices = HashMap::new();
        for (col_name, block) in blocks {
            pool_indices.insert(col_name, pool.lock().unwrap().set_initial_block(block));
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

    pub fn select(&self, column_exprs: &[ColumnExpr]) -> Result<DataFrame> {
        let operation = Operation::Select(column_exprs.to_vec());
        let pool_indices = column_exprs
            .iter()
            .map(|expr| if let Some(col_name) = expr.name() {
                let source_col = expr.source_name().ok_or_else(
                    || Error::WithoutSource(col_name.clone()),
                )?;
                let parent_idx = self.pool_indices.get(&source_col).ok_or_else(|| {
                    Error::MissingColumnInIndices(col_name.clone())
                })?;
                let idx = operation.hash_from_seed(parent_idx, &col_name);
                Ok((col_name, idx))
            } else {
                Err(Error::AliasRequired)
            })
            .collect::<Result<HashMap<String, u64>>>()?;
        let schema = Schema::from_owned(column_exprs
            .iter()
            .map(|expr| {
                let col_name = expr.name().unwrap();
                let type_ = expr.type_(&self.schema)?;
                Ok((col_name, type_))
            })
            .collect::<Result<Vec<(String, Type)>>>()?);
        Ok(DataFrame {
            pool_indices,
            schema: schema,
            parent: Some(box self.clone()),
            operation: Some(operation),
            ordered_by: vec![],
            grouped_by: vec![],
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
        if column_names.is_empty() {
            return Err(Error::OrderByWithNoSortColumns);
        }
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
            Operation::Select(ref column_exprs) => self.select(column_exprs)?,
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
                    (&Type::Bool, Some(value @ Value::Bool(_))) |
                    (&Type::Int, Some(value @ Value::Int(_))) |
                    (&Type::Float, Some(value @ Value::Float(_))) |
                    (&Type::String, Some(value @ Value::String(_))) |
                    (&Type::List(box Type::Bool), Some(value @ Value::BoolList(_))) |
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

    pub fn as_blocks(&self, pool: &PoolRef) -> Result<HashMap<String, AnyBlock>> {
        if self.should_materialize(pool) {
            self.materialize(pool)?;
        }

        let mut results = HashMap::new();
        for (name, idx) in &self.pool_indices {
            results.insert(
                name.clone(),
                pool.lock().unwrap().get_entry(idx)?.block.into_any_block(),
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
            let mut blocks = format.read(path, &self.schema)?;
            for (col_name, idx) in &self.pool_indices {
                pool.lock().unwrap().set_block(
                    *idx,
                    Arc::from(
                        blocks.remove(col_name).unwrap(),
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
        let id = timer_start!(&format!("materialize - {:?}", operation));

        // FIXME: don't hold the lock for all of materialize
        let mut pool = pool.lock().unwrap();

        match *operation {
            Operation::Select(ref column_exprs) => {
                let named = column_exprs
                    .into_iter()
                    .map(|expr| (expr.name().unwrap(), expr))
                    .collect::<HashMap<String, &ColumnExpr>>();
                let len = match ColumnExpr::source_names(
                    &column_exprs.iter().collect::<Vec<&ColumnExpr>>(),
                ).first() {
                    Some(col_name) => pool.get_entry(&parent.get_idx(col_name)?)?.block.len(),
                    None => 1,
                };
                for (col_name, idx) in &self.pool_indices {
                    let entry =
                        Self::eval_column(named[col_name], &pool, &parent.pool_indices, len)?;
                    pool.set_block(*idx, entry.block, entry.sorted)
                }
            }
            Operation::Filter(ref filter_col_name, ref predicate) => {
                let filter_col_idx = parent.get_idx(filter_col_name)?;
                let filter_entry = pool.get_entry(&filter_col_idx)?;
                let (pass_indices, filtered_block) = filter_entry.block.filter(predicate)?;
                pool.set_block(
                    operation.hash_from_seed(&filter_col_idx, filter_col_name),
                    Arc::from(filtered_block),
                    filter_entry.sorted,
                );

                let cpu_pool = CpuPool::new_num_cpus();
                let pass_indices_arc = Arc::new(pass_indices);
                let mut futures = vec![];
                for (col_name, idx) in &parent.pool_indices {
                    if col_name != filter_col_name {
                        let entry = pool.get_entry(idx)?;
                        futures.push(cpu_pool.spawn(filter_future(
                            operation.hash_from_seed(idx, col_name),
                            entry.block,
                            pass_indices_arc.clone(),
                            entry.sorted,
                        )));
                    }
                }
                future::join_all(futures)
                    .map(|blocks| for (idx, sorted, block) in blocks {
                        pool.set_block(idx, Arc::from(block), sorted)
                    })
                    .wait()
                    .unwrap();
            }
            Operation::OrderBy(ref col_names) => {
                let mut len = 0;
                let mut sort_columns = vec![];
                for col_name in col_names {
                    let parent_idx = parent.get_idx(col_name)?;
                    let parent_entry = pool.get_entry(&parent_idx)?;
                    len = parent_entry.block.len();
                    sort_columns.push(parent_entry.block);
                }

                let mut ordered_indices = (0..len).into_iter().collect::<Vec<usize>>();
                ordered_indices.sort_by(|&left_idx, &right_idx| {
                    for block in &sort_columns {
                        match block.cmp_at_indices(left_idx, right_idx) {
                            cmp::Ordering::Equal => (),
                            cmp::Ordering::Greater => return cmp::Ordering::Greater,
                            cmp::Ordering::Less => return cmp::Ordering::Less,
                        };
                    }
                    cmp::Ordering::Equal
                });
                let mut sort_order = vec![0usize; len];
                for (sort_idx, row_idx) in ordered_indices.into_iter().enumerate() {
                    sort_order[row_idx] = sort_idx
                }

                let cpu_pool = CpuPool::new_num_cpus();
                let sort_order_arc = Arc::new(sort_order);
                let mut futures = vec![];
                for column in self.schema.iter() {
                    let idx = parent.get_idx(&column.name)?;
                    let entry = pool.get_entry(&idx)?;
                    futures.push(cpu_pool.spawn(order_by_future(
                        operation.hash_from_seed(&idx, &column.name),
                        entry.block,
                        sort_order_arc.clone(),
                    )));
                }
                future::join_all(futures)
                    .map(|blocks| for (idx, (pool_idx, block)) in
                        blocks.into_iter().enumerate()
                    {
                        pool.set_block(pool_idx, Arc::from(block), idx == 0)
                    })
                    .wait()
                    .unwrap();
            }
            Operation::GroupBy(ref col_names) => {
                let mut len = 0;
                let mut group_columns = vec![];
                for col_name in col_names {
                    let parent_idx = parent.get_idx(col_name)?;
                    let parent_entry = pool.get_entry(&parent_idx)?;
                    len = parent_entry.block.len();
                    group_columns.push(parent_entry.block);
                }

                let mut group_offsets = vec![];
                for row_idx in 0..(len - 1) {
                    let next_is_different = group_columns
                        .iter()
                        .map(|block| {
                            block.cmp_at_indices(row_idx, row_idx + 1) == cmp::Ordering::Equal
                        })
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
                        pool.set_block(
                            idx,
                            Arc::from(parent_entry.block.select_by_idx(&group_offsets)),
                            parent_entry.sorted,
                        );
                    } else {
                        pool.set_block(
                            idx,
                            Arc::from(parent_entry.block.group_by(&group_offsets)),
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
                        pool.set_block(
                            new_idx,
                            Arc::from(entry.block.aggregate(aggregator)?),
                            false,
                        )
                    } else {
                        pool.set_block(new_idx, entry.block, entry.sorted)
                    }
                }
            }
            _ => unreachable!(),
        }
        timer_stop!(id);
        Ok(())
    }

    fn get_idx(&self, col_name: &str) -> Result<u64> {
        self.pool_indices
            .get(col_name)
            .ok_or_else(|| Error::MissingColumnInIndices(col_name.to_string()))
            .map(|idx| *idx)
    }

    fn eval_column(
        column_expr: &ColumnExpr,
        pool: &Pool,
        parent_indices: &HashMap<String, u64>,
        len: usize,
    ) -> Result<Entry> {
        match *column_expr {
            ColumnExpr::Constant(ref value) => {
                let mut builder = block::builder(&value.type_());
                for _ in 0..len {
                    builder.push(value.clone())?
                }
                Ok(Entry::new(Arc::from(builder.build()), true))
            }
            ColumnExpr::Source(ref col_name) => {
                let parent_idx = parent_indices.get(col_name).ok_or_else(|| {
                    Error::MissingColumnInIndices(col_name.clone())
                })?;
                let parent_entry = pool.get_entry(parent_idx)?;
                Ok(parent_entry)
            }
            ColumnExpr::Alias(_, ref child_expr) => {
                Self::eval_column(child_expr, pool, parent_indices, len)
            }
            ColumnExpr::Operation(ref operation, ref left_expr, ref right_expr) => {
                let left = Self::eval_column(left_expr, pool, parent_indices, len)?;
                let right = Self::eval_column(right_expr, pool, parent_indices, len)?;
                Ok(Entry::new(
                    Arc::from(
                        left.block.combine(right.block.as_ref(), operation)?,
                    ),
                    false,
                ))
            }
        }
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
