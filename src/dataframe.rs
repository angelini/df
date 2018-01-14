use pool::Pool;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;
use value::{Predicate, Type, Value, Values};

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

#[derive(Clone, Hash)]
pub enum Aggregator {
    First,
    Sum,
    Max,
    Min,
}

impl Aggregator {
    fn output_type(&self, input_type: &Type) -> Type {
        match *self {
            Aggregator::Sum => {
                if input_type == &Type::Int {
                    Type::Int
                } else {
                    panic!(format!(
                        "Aggregator type error: cannot sum {:?}",
                        input_type
                    ))
                }
            }
            Aggregator::First | Aggregator::Max | Aggregator::Min => input_type.clone(),
        }
    }

    fn aggregate(&self, input: Values) -> Value {
        match *self {
            Aggregator::First => {
                match input {
                    Values::Boolean(values) => Value::Boolean(Self::first(&values)),
                    Values::Int(values) => Value::Int(Self::first(&values)),
                    Values::String(values) => Value::String(Self::first(&values)),
                }
            }
            Aggregator::Sum => {
                match input {
                    Values::Int(values) => Value::Int(values.iter().fold(0, |acc, &v| acc + v)),
                    _ => panic!("Aggregator type error: cannot sum {:?}", input),
                }
            }
            Aggregator::Max => {
                match input {
                    Values::Boolean(values) => Value::Boolean(Self::max(&values)),
                    Values::Int(values) => Value::Int(Self::max(&values)),
                    Values::String(values) => Value::String(Self::max(&values)),
                }
            }
            Aggregator::Min => {
                match input {
                    Values::Boolean(values) => Value::Boolean(Self::min(&values)),
                    Values::Int(values) => Value::Int(Self::min(&values)),
                    Values::String(values) => Value::String(Self::min(&values)),
                }
            }
        }
    }

    fn first<T: Clone>(values: &[T]) -> T {
        match values.first() {
            Some(v) => v.clone(),
            None => panic!("Agg error: first on empty column"),
        }
    }

    fn max<T: Clone + Ord>(values: &[T]) -> T {
        match values.iter().max() {
            Some(v) => v.clone(),
            None => panic!("Agg error: max on empty column"),
        }
    }

    fn min<T: Clone + Ord>(values: &[T]) -> T {
        match values.iter().min() {
            Some(v) => v.clone(),
            None => panic!("Agg error: min on empty column"),
        }
    }
}

#[derive(Clone, Hash)]
enum Operation {
    Select(Vec<String>),
    Filter(String, Predicate),
    Sort(Vec<String>),
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

    pub fn select(&self, column_names: &[&str]) -> DataFrame {
        let operation = Operation::Select(column_names.iter().map(|s| s.to_string()).collect());
        let pool_indices = self.pool_indices
            .iter()
            .filter(|&(k, _)| column_names.contains(&k.as_str()))
            .map(|(k, v)| (k.clone(), operation.hash_from_seed(v, k)))
            .collect();
        DataFrame {
            pool_indices,
            schema: self.schema.select(column_names),
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
        }
    }

    pub fn filter(&self, filter_column_name: &str, predicate: Predicate) -> DataFrame {
        let operation = Operation::Filter(filter_column_name.to_string(), predicate);
        let pool_indices = self.pool_indices
            .iter()
            .map(|(col_name, idx)| {
                (col_name.clone(), operation.hash_from_seed(idx, col_name))
            })
            .collect();
        DataFrame {
            pool_indices,
            schema: self.schema.clone(),
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
        }
    }

    pub fn sort(&self, column_names: &[&str]) -> DataFrame {
        let operation = Operation::Sort(column_names.iter().map(|s| s.to_string()).collect());
        let pool_indices = self.pool_indices
            .iter()
            .map(|(col_name, idx)| {
                (col_name.clone(), operation.hash_from_seed(idx, col_name))
            })
            .collect();
        DataFrame {
            pool_indices,
            schema: self.schema.clone(),
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
        }
    }

    pub fn aggregate(&self, aggregators: BTreeMap<String, Aggregator>) -> DataFrame {
        {
            let missing: HashSet<&String> = &HashSet::from_iter(self.schema.columns.keys()) -
                &HashSet::from_iter(aggregators.keys());
            if !missing.is_empty() {
                panic!(format!("Missing aggregates for columns: {:?}", missing))
            }
        }
        let columns = self.schema
            .columns
            .iter()
            .map(|(name, column)| {
                let aggregator = &aggregators[name];
                (
                    name.clone(),
                    Column::new(aggregator.output_type(&column.type_)),
                )
            })
            .collect::<BTreeMap<String, Column>>();
        let operation = Operation::Aggregation(aggregators);
        let pool_indices = self.pool_indices
            .iter()
            .map(|(col_name, idx)| {
                (col_name.clone(), operation.hash_from_seed(idx, col_name))
            })
            .collect();
        DataFrame {
            pool_indices,
            schema: Schema { columns },
            parent: Some(Box::new(self.clone())),
            operation: Some(operation),
        }
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
            rows.push(Row { values: row_values });
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
                        "Parent column missing pool_indices: {}",
                        col_name
                    ));
                    let parent_entry = pool.get_entry(parent_idx).expect(&format!(
                        "Parent entry missing in pool: {}",
                        parent_idx
                    ));
                    pool.set_values(*idx, parent_entry.values, parent_entry.sorted)
                }
            }
            Operation::Filter(ref filter_col_name, ref predicate) => {
                let filter_col_idx = parent.pool_indices.get(filter_col_name).expect(&format!(
                    "Column missing in pool_indices: filter col => {}",
                    filter_col_name
                ));

                let filter_entry = pool.get_entry(filter_col_idx).expect(&format!(
                    "Entry missing in pool: filter col idx {}",
                    filter_col_idx
                ));

                let (filter_pass_idxs, filtered_values) = filter_entry.values.filter(predicate);
                pool.set_values(
                    operation.hash_from_seed(filter_col_idx, filter_col_name),
                    filtered_values,
                    filter_entry.sorted,
                );

                for (col_name, idx) in &parent.pool_indices {
                    if col_name != filter_col_name {
                        let new_idx = operation.hash_from_seed(idx, col_name);
                        let entry = pool.get_entry(idx).expect(&format!(
                            "Parent entry missing in pool: {}",
                            col_name
                        ));
                        let values = entry.values.select_by_idx(&filter_pass_idxs);
                        pool.set_values(new_idx, values, entry.sorted)
                    }
                }
            }
            Operation::Sort(ref col_names) => {
                let mut parent_sorting: Option<Vec<usize>> = None;
                for col_name in col_names {
                    let col_idx = parent.pool_indices.get(col_name).expect(&format!(
                        "Column missing in pool_indices: filter col => {}",
                        col_name
                    ));
                    let entry = pool.get_entry(col_idx).expect(&format!(
                        "Entry missing in pool: filter col idx {}",
                        col_idx
                    ));
                    let (sort_indices, values) =
                        entry.values.sort(
                            &parent_sorting.as_ref().map(|s| s.as_slice()),
                            false,
                        );
                    pool.set_values(
                        operation.hash_from_seed(col_idx, col_name),
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
                            let col_idx = parent.pool_indices.get(col_name).expect(&format!(
                                "Column missing in pool_indices: filter col => {}",
                                col_name
                            ));
                            let entry = pool.get_entry(col_idx).expect(&format!(
                                "Entry missing in pool: filter col idx {}",
                                col_idx
                            ));
                            let (_, values) =
                                entry.values.sort(
                                    &parent_sorting.as_ref().map(|s| s.as_slice()),
                                    true,
                                );
                            pool.set_values(
                                operation.hash_from_seed(col_idx, col_name),
                                values,
                                false,
                            );
                        }
                    }
                    None => panic!("Sorting with no sort columns"),
                }
            }
            Operation::Aggregation(ref aggregators) => {
                for (col_name, idx) in &parent.pool_indices {
                    let aggregator = &aggregators[col_name];
                    let new_idx = operation.hash_from_seed(idx, col_name);
                    let entry = pool.get_entry(idx).expect(&format!(
                        "Parent entry missing: {}",
                        col_name
                    ));
                    pool.set_values(
                        new_idx,
                        Values::from(aggregator.aggregate(entry.values)),
                        true,
                    )
                }
            }
        }
    }
}
