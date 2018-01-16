use decorum::R64;
use std::fmt;
use std::rc::Rc;
use std::result;

#[derive(Debug)]
pub enum Error {
    PredicateAndValueTypes(Type, Type),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::PredicateAndValueTypes(ref predicate_type, ref value_type) => {
                write!(
                    f,
                    "Predicate type ({:?}) and value type ({:?}) mismatch",
                    predicate_type,
                    value_type
                )
            }
        }
    }
}

type Result<T> = result::Result<T, Error>;

#[derive(Clone, Debug, PartialEq)]
pub enum Type {
    Boolean,
    Int,
    Float,
    String,
    List(Box<Type>),
}

#[derive(Clone, Debug, Hash)]
pub enum Value {
    Boolean(bool),
    Int(u64),
    Float(R64),
    String(String),
}

impl Value {
    pub fn type_(&self) -> Type {
        match *self {
            Value::Boolean(_) => Type::Boolean,
            Value::Int(_) => Type::Int,
            Value::Float(_) => Type::Float,
            Value::String(_) => Type::String,
        }
    }
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

impl From<R64> for Value {
    fn from(value: R64) -> Self {
        Value::Float(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::String(value)
    }
}

fn gen_select_by_idx<T: Clone>(values: &[T], indices: &[usize]) -> Vec<T> {
    values
        .iter()
        .enumerate()
        .filter(|&(k, _)| indices.contains(&k))
        .map(|(_, v)| v.clone())
        .collect()
}

#[derive(Clone, Debug)]
pub enum ListValues {
    Boolean(Vec<Vec<bool>>),
    Int(Vec<Vec<u64>>),
    Float(Vec<Vec<R64>>),
    String(Vec<Vec<String>>),
}

impl ListValues {
    pub fn type_(&self) -> Type {
        match *self {
            ListValues::Boolean(_) => Type::Boolean,
            ListValues::Int(_) => Type::Int,
            ListValues::Float(_) => Type::Float,
            ListValues::String(_) => Type::String,
        }
    }

    pub fn len(&self) -> usize {
        match *self {
            ListValues::Boolean(ref values) => values.len(),
            ListValues::Int(ref values) => values.len(),
            ListValues::Float(ref values) => values.len(),
            ListValues::String(ref values) => values.len(),
        }
    }

    pub fn select_by_idx(&self, indices: &[usize]) -> ListValues {
        match *self {
            ListValues::Boolean(ref values) => ListValues::Boolean(
                gen_select_by_idx(values, indices),
            ),
            ListValues::Int(ref values) => ListValues::Int(gen_select_by_idx(values, indices)),
            ListValues::Float(ref values) => ListValues::Float(gen_select_by_idx(values, indices)),
            ListValues::String(ref values) => ListValues::String(
                gen_select_by_idx(values, indices),
            ),
        }
    }
}

#[derive(Clone, Debug)]
pub enum Values {
    Boolean(Rc<Vec<bool>>),
    Int(Rc<Vec<u64>>),
    Float(Rc<Vec<R64>>),
    String(Rc<Vec<String>>),
    List(Rc<ListValues>),
}

impl Values {
    pub fn type_(&self) -> Type {
        match *self {
            Values::Boolean(_) => Type::Boolean,
            Values::Int(_) => Type::Int,
            Values::Float(_) => Type::Float,
            Values::String(_) => Type::String,
            Values::List(ref list) => list.type_(),
        }
    }

    pub fn select_by_idx(&self, indices: &[usize]) -> Values {
        match *self {
            Values::Boolean(ref values) => Values::from(gen_select_by_idx(values, indices)),
            Values::Int(ref values) => Values::from(gen_select_by_idx(values, indices)),
            Values::Float(ref values) => Values::from(gen_select_by_idx(values, indices)),
            Values::String(ref values) => Values::from(gen_select_by_idx(values, indices)),
            Values::List(ref values) => Values::from(values.select_by_idx(indices)),
        }
    }

    pub fn sort(
        &self,
        parent_sorting: &Option<&[usize]>,
        only_use_parent: bool,
    ) -> (Vec<usize>, Values) {
        match *self {
            Values::Boolean(ref values) => {
                let (indices, sorted_values) =
                    Self::gen_sort(values, parent_sorting, only_use_parent);
                (indices, Values::from(sorted_values))
            }
            Values::Int(ref values) => {
                let (indices, sorted_values) =
                    Self::gen_sort(values, parent_sorting, only_use_parent);
                (indices, Values::from(sorted_values))
            }
            Values::Float(ref values) => {
                let (indices, sorted_values) =
                    Self::gen_sort(values, parent_sorting, only_use_parent);
                (indices, Values::from(sorted_values))
            }
            Values::String(ref values) => {
                let (indices, sorted_values) =
                    Self::gen_sort(values, parent_sorting, only_use_parent);
                (indices, Values::from(sorted_values))
            }
            Values::List(_) => unimplemented!(),
        }
    }

    fn gen_sort<T: Clone + Ord>(
        values: &[T],
        parent_sorting: &Option<&[usize]>,
        only_use_parent: bool,
    ) -> (Vec<usize>, Vec<T>) {
        let mut sorted = values.iter().enumerate().collect::<Vec<(usize, &T)>>();
        sorted.sort_by(|&(left_idx, left_value), &(right_idx, right_value)| {
            match *parent_sorting {
                Some(sort_indices) => {
                    let left_sort = sort_indices[left_idx];
                    let right_sort = sort_indices[right_idx];
                    if left_sort == right_sort && !only_use_parent {
                        left_value.cmp(right_value)
                    } else {
                        left_sort.cmp(&right_sort)
                    }
                }
                None => left_value.cmp(right_value),
            }
        });
        (
            sorted.iter().map(|&(k, _)| k).collect(),
            sorted.into_iter().map(|(_, v)| v.clone()).collect(),
        )
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

impl From<Vec<R64>> for Values {
    fn from(values: Vec<R64>) -> Self {
        Values::Float(Rc::new(values))
    }
}

impl From<Vec<String>> for Values {
    fn from(values: Vec<String>) -> Self {
        Values::String(Rc::new(values))
    }
}

impl From<ListValues> for Values {
    fn from(values: ListValues) -> Self {
        Values::List(Rc::new(values))
    }
}

impl From<Value> for Values {
    fn from(value: Value) -> Self {
        match value {
            Value::Boolean(value) => Values::Boolean(Rc::new(vec![value])),
            Value::Int(value) => Values::Int(Rc::new(vec![value])),
            Value::Float(value) => Values::Float(Rc::new(vec![value])),
            Value::String(value) => Values::String(Rc::new(vec![value])),
        }
    }
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
    pub fn new(comparator: Comparator, value: Value) -> Predicate {
        Predicate { comparator, value }
    }

    pub fn filter(&self, values: &Values) -> Result<(Vec<usize>, Values)> {
        match (values, &self.value) {
            (&Values::Boolean(ref values), &Value::Boolean(ref value)) => {
                let (indices, filtered_values) = Self::gen_filter(&self.comparator, values, value);
                Ok((indices, Values::from(filtered_values)))
            }
            (&Values::Int(ref values), &Value::Int(ref value)) => {
                let (indices, filtered_values) = Self::gen_filter(&self.comparator, values, value);
                Ok((indices, Values::from(filtered_values)))
            }
            (&Values::Float(ref values), &Value::Float(ref value)) => {
                let (indices, filtered_values) = Self::gen_filter(&self.comparator, values, value);
                Ok((indices, Values::from(filtered_values)))
            }
            (&Values::String(ref values), &Value::String(ref value)) => {
                let (indices, filtered_values) = Self::gen_filter(&self.comparator, values, value);
                Ok((indices, Values::from(filtered_values)))
            }
            (&Values::List(ref values), list_values) => unimplemented!(),
            (values, value) => Err(Error::PredicateAndValueTypes(values.type_(), value.type_())),
        }
    }

    fn gen_filter<T: Clone + Ord>(
        comparator: &Comparator,
        values: &[T],
        value: &T,
    ) -> (Vec<usize>, Vec<T>) {
        let filtered = values
            .iter()
            .enumerate()
            .filter(|&(_, v)| comparator.pass(v, value))
            .map(|(k, v)| (k, v.clone()))
            .collect::<Vec<(usize, T)>>();
        (
            filtered.iter().map(|&(k, _)| k).collect(),
            filtered.into_iter().map(|(_, v)| v).collect(),
        )
    }
}
