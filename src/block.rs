use std::cmp;
use std::f64;
use std::fmt;
use std::result;
use std::str;

use fnv;

use aggregate::{self, Aggregator};
use value::{Comparator, Predicate, Nullable, Type, Value};

#[derive(Debug)]
pub enum Error {
    PushType(Type, Value),
    PredicateAndValueTypes(Type, Type),
    Aggregate(aggregate::Error),
}

impl From<aggregate::Error> for Error {
    fn from(error: aggregate::Error) -> Error {
        Error::Aggregate(error)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::PushType(ref col, ref val) => {
                write!(f, "Error pushing value {:?} to a column of type {:?}", val, col)
            }
            Error::PredicateAndValueTypes(ref predicate_type, ref value_type) => {
                write!(
                    f,
                    "Predicate type ({:?}) and value type ({:?}) mismatch",
                    predicate_type,
                    value_type
                )
            }
            Error::Aggregate(ref err) => write!(f, "{}", err),
        }
    }
}

type Result<T> = result::Result<T, Error>;

type SortScores = fnv::FnvHashMap<usize, usize>;

#[derive(Debug, Deserialize, Serialize)]
pub enum AnyBlock {
    Boolean(Vec<bool>),
    Int(Vec<i64>),
    Float(Vec<f64>),
    String(Vec<String>),
    BooleanList(Vec<Vec<bool>>),
    IntList(Vec<Vec<i64>>),
    FloatList(Vec<Vec<f64>>),
    StringList(Vec<Vec<String>>),
}

impl AnyBlock {
    pub fn into_block(self) -> Box<Block> {
        match self {
            AnyBlock::Boolean(values) => box BooleanBlock::new(values),
            AnyBlock::Int(values) => box IntBlock::new(values),
            AnyBlock::Float(values) => box FloatBlock::new(values),
            AnyBlock::String(values) => box StringBlock::new(values),
            AnyBlock::BooleanList(values) => box ListBlock::Boolean(values),
            AnyBlock::IntList(values) => box ListBlock::Int(values),
            AnyBlock::FloatList(values) => box ListBlock::Float(values),
            AnyBlock::StringList(values) => box ListBlock::String(values),
        }
    }
}

macro_rules! from_anyblock {
    ( $([$name:ident, $typ:ty]),* ) => {
        $(
            impl From<$typ> for AnyBlock {
                fn from(values: $typ) -> AnyBlock {
                    AnyBlock::$name(values)
                }
            }
        )*
    };
}

from_anyblock!(
    [Boolean, Vec<bool>],
    [Int, Vec<i64>],
    [Float, Vec<f64>],
    [String, Vec<String>],
    [BooleanList, Vec<Vec<bool>>],
    [IntList, Vec<Vec<i64>>],
    [FloatList, Vec<Vec<f64>>],
    [StringList, Vec<Vec<String>>]
);

pub trait Block: fmt::Debug {
    fn type_(&self) -> Type;
    fn len(&self) -> usize;

    fn equal_at_idxs(&self, usize, usize) -> bool;
    fn select_by_idx(&self, &[usize]) -> Box<Block>;

    fn get(&self, usize) -> Value;
    fn push(&mut self, Value) -> Result<()>;

    fn filter(&self, &Predicate) -> Result<(Vec<usize>, Box<Block>)>;
    fn order_by(&self, &Option<SortScores>, bool) -> (SortScores, Box<Block>);
    fn group_by(&self, &[usize]) -> Box<Block>;
    fn aggregate(&self, &Aggregator) -> Result<Box<Block>>;

    fn into_any_block(&self) -> AnyBlock;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn gen_select_by_idx<T: Clone>(values: &[T], indices: &[usize]) -> Vec<T> {
    indices
        .into_iter()
        .map(|idx| values[*idx].clone())
        .collect()
}

fn gen_filter<T: Clone + PartialEq + PartialOrd>(
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
    filtered.into_iter().unzip()
}

fn nullable_partial_cmp<T: Nullable + PartialOrd>(left: &T, right: &T) -> cmp::Ordering {
    left.partial_cmp(right).unwrap_or_else(
        || if left.is_null() {
            cmp::Ordering::Less
        } else {
            cmp::Ordering::Greater
        },
    )
}

fn gen_order_by<T: Clone + Nullable + PartialOrd>(
    values: &[T],
    sort_scores: &Option<SortScores>,
    only_use_score: bool,
) -> (SortScores, Vec<T>) {
    if values.is_empty() {
        return (fnv::FnvHashMap::default(), vec![]);
    }

    let sorted = match *sort_scores {
        Some(ref sort_scores) => {
            let mut buffer = values
                .iter()
                .enumerate()
                .map(|(idx, v)| (idx, sort_scores[&idx], v))
                .collect::<Vec<(usize, usize, &T)>>();
            buffer.sort_by(|&(_, left_score, left_value),
             &(_, right_score, right_value)| {
                if left_score == right_score && !only_use_score {
                    nullable_partial_cmp(left_value, right_value)
                } else {
                    left_score.cmp(&right_score)
                }
            });
            buffer
                .into_iter()
                .map(|(idx, _, value)| (idx, value))
                .collect::<Vec<(usize, &T)>>()
        }
        None => {
            let mut buffer = values.iter().enumerate().collect::<Vec<(usize, &T)>>();
            buffer.sort_by(|&(_, left_value), &(_, right_value)| {
                nullable_partial_cmp(left_value, right_value)
            });
            buffer
        }
    };

    let mut new_scores = fnv::FnvHashMap::default();
    let mut previous_score = 0;
    new_scores.insert(sorted[0].0, 0);
    for (idx, &(row_idx, value)) in sorted.iter().enumerate().skip(1) {
        if value == sorted[idx - 1].1 {
            new_scores.insert(row_idx, previous_score);
        } else {
            previous_score += 1;
            new_scores.insert(row_idx, previous_score);
        }
    }
    (
        new_scores,
        sorted.into_iter().map(|(_, v)| v.clone()).collect(),
    )
}

fn gen_group_by<T: Clone>(values: &[T], group_offsets: &[usize]) -> Vec<Vec<T>> {
    let mut offset_index = 0;
    let mut outer = vec![];
    let mut inner = vec![];
    for (idx, value) in values.iter().enumerate() {
        inner.push(value.clone());
        if idx == group_offsets[offset_index] {
            outer.push(inner);
            inner = vec![];
            offset_index += 1;
        }
    }
    outer
}

#[derive(Clone, Debug)]
enum ListBlock {
    Boolean(Vec<Vec<bool>>),
    Int(Vec<Vec<i64>>),
    Float(Vec<Vec<f64>>),
    String(Vec<Vec<String>>),
}

impl ListBlock {
    fn average(&self) -> Result<Box<Block>> {
        match *self {
            ListBlock::Boolean(_) => Err(Error::from(aggregate::Error::AggregatorAndColumnType(
                Aggregator::Average,
                Type::Boolean,
            ))),
            ListBlock::Int(ref values) => {
                Ok(box FloatBlock::new(
                    values
                        .iter()
                        .map(|vs| vs.iter().sum::<i64>() as f64 / vs.len() as f64)
                        .collect(),
                ))
            }
            ListBlock::Float(ref values) => {
                Ok(box FloatBlock::new(
                    values
                        .iter()
                        .map(|vs| vs.iter().sum::<f64>() as f64 / vs.len() as f64)
                        .collect(),
                ))
            }
            ListBlock::String(_) => Err(Error::from(aggregate::Error::AggregatorAndColumnType(
                Aggregator::Average,
                Type::String,
            ))),
        }
    }

    fn count(&self) -> Result<Box<Block>> {
        match *self {
            ListBlock::Boolean(ref values) => Ok(box IntBlock::new(
                values.iter().map(|vs| vs.len() as i64).collect(),
            )),
            ListBlock::Int(ref values) => Ok(box IntBlock::new(
                values.iter().map(|vs| vs.len() as i64).collect(),
            )),
            ListBlock::Float(ref values) => Ok(box IntBlock::new(
                values.iter().map(|vs| vs.len() as i64).collect(),
            )),
            ListBlock::String(ref values) => Ok(box IntBlock::new(
                values.iter().map(|vs| vs.len() as i64).collect(),
            )),
        }
    }

    fn sum(&self) -> Result<Box<Block>> {
        match *self {
            ListBlock::Boolean(_) => Err(Error::from(aggregate::Error::AggregatorAndColumnType(
                Aggregator::Sum,
                Type::Boolean,
            ))),
            ListBlock::Int(ref values) => {
                Ok(box IntBlock::new(
                    values.iter().map(|vs| vs.iter().sum()).collect(),
                ))
            }
            ListBlock::Float(ref values) => {
                Ok(box FloatBlock::new(
                    values.iter().map(|vs| vs.iter().sum()).collect(),
                ))
            }
            ListBlock::String(_) => Err(Error::from(aggregate::Error::AggregatorAndColumnType(
                Aggregator::Sum,
                Type::String,
            ))),
        }
    }
}

macro_rules! simple_list_aggregate {
    ($block:expr, $fn:expr) => {{
        match *$block {
            ListBlock::Boolean(ref values) => Ok(box BooleanBlock::new(values
                .iter()
                .map(|vs| $fn(vs))
                .collect::<aggregate::Result<Vec<bool>>>()?)),
            ListBlock::Int(ref values) => Ok(box IntBlock::new(values
                .iter()
                .map(|vs| $fn(vs))
                .collect::<aggregate::Result<Vec<i64>>>()?)),
            ListBlock::Float(ref values) => Ok(box FloatBlock::new(values
                .iter()
                .map(|vs| $fn(vs))
                .collect::<aggregate::Result<Vec<f64>>>()?)),
            ListBlock::String(ref values) => Ok(box StringBlock::new(values
                .iter()
                .map(|vs| $fn(vs))
                .collect::<aggregate::Result<Vec<String>>>()?)),
        }
    }};
}

impl Block for ListBlock {
    fn type_(&self) -> Type {
        match *self {
            ListBlock::Boolean(_) => Type::List(box Type::Boolean),
            ListBlock::Int(_) => Type::List(box Type::Int),
            ListBlock::Float(_) => Type::List(box Type::Float),
            ListBlock::String(_) => Type::List(box Type::String),
        }
    }

    fn len(&self) -> usize {
        match *self {
            ListBlock::Boolean(ref values) => values.len(),
            ListBlock::Int(ref values) => values.len(),
            ListBlock::Float(ref values) => values.len(),
            ListBlock::String(ref values) => values.len(),
        }
    }

    fn equal_at_idxs(&self, left: usize, right: usize) -> bool {
        match *self {
            ListBlock::Boolean(ref values) => values[left] == values[right],
            ListBlock::Int(ref values) => values[left] == values[right],
            ListBlock::Float(ref values) => values[left] == values[right],
            ListBlock::String(ref values) => values[left] == values[right],
        }
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        match *self {
            ListBlock::Boolean(ref values) => {
                box ListBlock::Boolean(gen_select_by_idx(values, indices))
            }
            ListBlock::Int(ref values) => box ListBlock::Int(gen_select_by_idx(values, indices)),
            ListBlock::Float(ref values) => {
                box ListBlock::Float(gen_select_by_idx(values, indices))
            }
            ListBlock::String(ref values) => {
                box ListBlock::String(gen_select_by_idx(values, indices))
            }
        }
    }

    fn get(&self, idx: usize) -> Value {
        match *self {
            ListBlock::Boolean(ref values) => Value::from(values[idx].clone()),
            ListBlock::Int(ref values) => Value::from(values[idx].clone()),
            ListBlock::Float(ref values) => Value::from(values[idx].clone()),
            ListBlock::String(ref values) => Value::from(values[idx].clone()),
        }
    }

    fn push(&mut self, _value: Value) -> Result<()> {
        unimplemented!()
    }

    fn filter(&self, _predicate: &Predicate) -> Result<(Vec<usize>, Box<Block>)> {
        unimplemented!()
    }

    fn order_by(
        &self,
        _sort_scores: &Option<fnv::FnvHashMap<usize, usize>>,
        _only_use_scores: bool,
    ) -> (SortScores, Box<Block>) {
        unimplemented!()
    }

    fn group_by(&self, _group_offsets: &[usize]) -> Box<Block> {
        unimplemented!()
    }

    fn aggregate(&self, aggregator: &Aggregator) -> Result<Box<Block>> {
        match *aggregator {
            Aggregator::Average => self.average(),
            Aggregator::Count => self.count(),
            Aggregator::First => simple_list_aggregate!(self, Aggregator::first),
            Aggregator::Max => simple_list_aggregate!(self, Aggregator::max),
            Aggregator::Min => simple_list_aggregate!(self, Aggregator::min),
            Aggregator::Sum => self.sum(),
        }
    }

    fn into_any_block(&self) -> AnyBlock {
        match *self {
            ListBlock::Boolean(ref values) => AnyBlock::BooleanList(values.clone()),
            ListBlock::Int(ref values) => AnyBlock::IntList(values.clone()),
            ListBlock::Float(ref values) => AnyBlock::FloatList(values.clone()),
            ListBlock::String(ref values) => AnyBlock::StringList(values.clone()),
        }
    }
}

#[derive(Clone, Debug)]
struct BooleanBlock {
    values: Vec<bool>,
}

impl BooleanBlock {
    fn new(values: Vec<bool>) -> Self {
        BooleanBlock { values }
    }
}

impl Block for BooleanBlock {
    fn type_(&self) -> Type {
        Type::Int
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn equal_at_idxs(&self, left: usize, right: usize) -> bool {
        self.values[left] == self.values[right]
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        box BooleanBlock::new(gen_select_by_idx(&self.values, indices))
    }

    fn get(&self, idx: usize) -> Value {
        Value::from(self.values[idx])
    }

    fn push(&mut self, value: Value) -> Result<()> {
        match value {
            Value::Boolean(v) => self.values.push(v),
            _ => return Err(Error::PushType(self.type_(), value))
        }
        Ok(())
    }

    fn filter(&self, predicate: &Predicate) -> Result<(Vec<usize>, Box<Block>)> {
        if let Value::Boolean(ref value) = predicate.value {
            let (indices, filtered) = gen_filter(&predicate.comparator, &self.values, value);
            Ok((indices, box BooleanBlock::new(filtered)))
        } else {
            Err(Error::PredicateAndValueTypes(
                predicate.value.type_(),
                self.type_(),
            ))
        }
    }

    fn order_by(
        &self,
        sort_scores: &Option<fnv::FnvHashMap<usize, usize>>,
        only_use_scores: bool,
    ) -> (SortScores, Box<Block>) {
        let (indices, ordered) = gen_order_by(&self.values, sort_scores, only_use_scores);
        (indices, box BooleanBlock::new(ordered))
    }

    fn group_by(&self, group_offsets: &[usize]) -> Box<Block> {
        box ListBlock::Boolean(gen_group_by(&self.values, group_offsets))
    }

    fn aggregate(&self, aggregator: &Aggregator) -> Result<Box<Block>> {
        match *aggregator {
            Aggregator::Count => Ok(box IntBlock::new(vec![self.len() as i64])),
            Aggregator::First => Ok(box BooleanBlock::new(
                vec![Aggregator::first(&self.values)?],
            )),
            Aggregator::Max => Ok(box BooleanBlock::new(vec![Aggregator::max(&self.values)?])),
            Aggregator::Min => Ok(box BooleanBlock::new(vec![Aggregator::min(&self.values)?])),
            Aggregator::Average | Aggregator::Sum => Err(Error::from(
                aggregate::Error::AggregatorAndColumnType(
                    aggregator.clone(),
                    self.type_(),
                ),
            )),
        }
    }

    fn into_any_block(&self) -> AnyBlock {
        AnyBlock::Boolean(self.values.clone())
    }
}

#[derive(Debug, Clone)]
struct IntBlock {
    values: Vec<i64>,
}

impl IntBlock {
    fn new(values: Vec<i64>) -> Self {
        IntBlock { values }
    }
}

impl Block for IntBlock {
    fn type_(&self) -> Type {
        Type::Int
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn equal_at_idxs(&self, left: usize, right: usize) -> bool {
        self.values[left] == self.values[right]
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        box IntBlock::new(gen_select_by_idx(&self.values, indices))
    }

    fn get(&self, idx: usize) -> Value {
        Value::from(self.values[idx])
    }

    fn push(&mut self, value: Value) -> Result<()> {
        match value {
            Value::Int(v) => self.values.push(v),
            _ => return Err(Error::PushType(self.type_(), value))
        }
        Ok(())
    }

    fn filter(&self, predicate: &Predicate) -> Result<(Vec<usize>, Box<Block>)> {
        if let Value::Int(ref value) = predicate.value {
            let (indices, filtered) = gen_filter(&predicate.comparator, &self.values, value);
            Ok((indices, box IntBlock::new(filtered)))
        } else {
            Err(Error::PredicateAndValueTypes(
                predicate.value.type_(),
                self.type_(),
            ))
        }
    }

    fn order_by(
        &self,
        sort_scores: &Option<fnv::FnvHashMap<usize, usize>>,
        only_use_scores: bool,
    ) -> (SortScores, Box<Block>) {
        let (indices, ordered) = gen_order_by(&self.values, sort_scores, only_use_scores);
        (indices, box IntBlock::new(ordered))
    }

    fn group_by(&self, group_offsets: &[usize]) -> Box<Block> {
        box ListBlock::Int(gen_group_by(&self.values, group_offsets))
    }


    fn aggregate(&self, aggregator: &Aggregator) -> Result<Box<Block>> {
        match *aggregator {
            Aggregator::Average => {
                let average = self.values.iter().sum::<i64>() as f64 / self.values.len() as f64;
                Ok(box FloatBlock::new(vec![average]))
            }
            Aggregator::Count => Ok(box IntBlock::new(vec![self.len() as i64])),
            Aggregator::First => Ok(box IntBlock::new(vec![Aggregator::first(&self.values)?])),
            Aggregator::Max => Ok(box IntBlock::new(vec![Aggregator::max(&self.values)?])),
            Aggregator::Min => Ok(box IntBlock::new(vec![Aggregator::min(&self.values)?])),
            Aggregator::Sum => Ok(box IntBlock::new(vec![self.values.iter().sum()])),
        }
    }

    fn into_any_block(&self) -> AnyBlock {
        AnyBlock::Int(self.values.clone())
    }
}

#[derive(Clone, Debug)]
struct FloatBlock {
    values: Vec<f64>,
}

impl FloatBlock {
    fn new(values: Vec<f64>) -> Self {
        FloatBlock { values }
    }
}

impl Block for FloatBlock {
    fn type_(&self) -> Type {
        Type::Float
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn equal_at_idxs(&self, left: usize, right: usize) -> bool {
        (self.values[left] - self.values[right]).abs() < f64::EPSILON
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        box FloatBlock::new(gen_select_by_idx(&self.values, indices))
    }

    fn get(&self, idx: usize) -> Value {
        Value::from(self.values[idx])
    }

    fn push(&mut self, value: Value) -> Result<()> {
        match value {
            Value::Float(v) => self.values.push(v.into_inner()),
            _ => return Err(Error::PushType(self.type_(), value))
        }
        Ok(())
    }

    fn filter(&self, predicate: &Predicate) -> Result<(Vec<usize>, Box<Block>)> {
        if let Value::Float(ref value) = predicate.value {
            let (indices, filtered) =
                gen_filter(&predicate.comparator, &self.values, &value.into_inner());
            Ok((indices, box FloatBlock::new(filtered)))
        } else {
            Err(Error::PredicateAndValueTypes(
                predicate.value.type_(),
                self.type_(),
            ))
        }
    }

    fn order_by(
        &self,
        sort_scores: &Option<fnv::FnvHashMap<usize, usize>>,
        only_use_scores: bool,
    ) -> (SortScores, Box<Block>) {
        let (indices, ordered) = gen_order_by(&self.values, sort_scores, only_use_scores);
        (indices, box FloatBlock::new(ordered))
    }

    fn group_by(&self, group_offsets: &[usize]) -> Box<Block> {
        box ListBlock::Float(gen_group_by(&self.values, group_offsets))
    }

    fn aggregate(&self, aggregator: &Aggregator) -> Result<Box<Block>> {
        match *aggregator {
            Aggregator::Average => {
                let average = self.values.iter().sum::<f64>() / self.values.len() as f64;
                Ok(box FloatBlock::new(vec![average]))
            }
            Aggregator::Count => Ok(box IntBlock::new(vec![self.len() as i64])),
            Aggregator::First => Ok(box FloatBlock::new(vec![Aggregator::first(&self.values)?])),
            Aggregator::Max => Ok(box FloatBlock::new(vec![Aggregator::max(&self.values)?])),
            Aggregator::Min => Ok(box FloatBlock::new(vec![Aggregator::min(&self.values)?])),
            Aggregator::Sum => Ok(box FloatBlock::new(vec![self.values.iter().sum()])),
        }
    }

    fn into_any_block(&self) -> AnyBlock {
        AnyBlock::Float(self.values.clone())
    }
}

#[derive(Clone, Debug)]
struct StringBlock {
    values: Vec<String>,
}

impl StringBlock {
    fn new(values: Vec<String>) -> Self {
        StringBlock { values }
    }
}

impl Block for StringBlock {
    fn type_(&self) -> Type {
        Type::String
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn equal_at_idxs(&self, left: usize, right: usize) -> bool {
        self.values[left] == self.values[right]
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        box StringBlock::new(gen_select_by_idx(&self.values, indices))
    }

    fn get(&self, idx: usize) -> Value {
        Value::from(self.values[idx].clone())
    }

    fn push(&mut self, value: Value) -> Result<()> {
        match value {
            Value::String(v) => self.values.push(v),
            _ => return Err(Error::PushType(self.type_(), value))
        }
        Ok(())
    }

    fn filter(&self, predicate: &Predicate) -> Result<(Vec<usize>, Box<Block>)> {
        if let Value::String(ref value) = predicate.value {
            let (indices, filtered) = gen_filter(&predicate.comparator, &self.values, value);
            Ok((indices, box StringBlock::new(filtered)))
        } else {
            Err(Error::PredicateAndValueTypes(
                predicate.value.type_(),
                self.type_(),
            ))
        }
    }

    fn order_by(
        &self,
        sort_scores: &Option<fnv::FnvHashMap<usize, usize>>,
        only_use_scores: bool,
    ) -> (SortScores, Box<Block>) {
        let (indices, ordered) = gen_order_by(&self.values, sort_scores, only_use_scores);
        (indices, box StringBlock::new(ordered))
    }

    fn group_by(&self, group_offsets: &[usize]) -> Box<Block> {
        box ListBlock::String(gen_group_by(&self.values, group_offsets))
    }

    fn aggregate(&self, aggregator: &Aggregator) -> Result<Box<Block>> {
        match *aggregator {
            Aggregator::Count => Ok(box IntBlock::new(vec![self.len() as i64])),
            Aggregator::First => Ok(box StringBlock::new(vec![Aggregator::first(&self.values)?])),
            Aggregator::Max => Ok(box StringBlock::new(vec![Aggregator::max(&self.values)?])),
            Aggregator::Min => Ok(box StringBlock::new(vec![Aggregator::min(&self.values)?])),
            Aggregator::Average | Aggregator::Sum => Err(Error::from(
                aggregate::Error::AggregatorAndColumnType(
                    aggregator.clone(),
                    self.type_(),
                ),
            )),
        }
    }

    fn into_any_block(&self) -> AnyBlock {
        AnyBlock::String(self.values.clone())
    }
}

pub fn empty_blocks(types: &[&Type]) -> Vec<Box<Block>> {
    let mut blocks: Vec<Box<Block>> = vec![];
    for type_ in types.iter() {
        match **type_ {
            Type::Boolean => blocks.push(box BooleanBlock::new(vec![])),
            Type::Int => blocks.push(box IntBlock::new(vec![])),
            Type::Float => blocks.push(box FloatBlock::new(vec![])),
            Type::String => blocks.push(box StringBlock::new(vec![])),
            _ => unimplemented!(),
        };
    }
    blocks
}


#[macro_export]
macro_rules! from_vecs {
    ( $p:expr, $( ($n:expr, $t:path, $v:expr) ),* ) => {{
        let schema = df::schema::Schema::new(
            &[ $( ($n, $t) ),* ]
        );
        let mut values = std::collections::HashMap::new();
        $(

            let block = df::block::AnyBlock::from($v);
            values.insert($n.to_string(), block.into_block());
        )*
        df::dataframe::DataFrame::new($p, schema, values)
    }};
    ( $p:expr, $( [$n:expr, $t:path, $v:expr] ),* ) => {{
        from_vecs!($p, $(($n, $t, $v)),*)
    }};
}
