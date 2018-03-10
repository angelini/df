use std::cmp;
use std::f64;
use std::fmt;
use std::mem;
use std::result;
use std::str;

use downcast_rs::Downcast;

use aggregate::{self, Aggregator};
use value::{Comparator, Nullable, Predicate, Type, Value};

#[derive(Debug)]
pub enum Error {
    ArithmeticTypes(ArithmeticOp, Type, Type),
    CombineSize(usize, usize),
    Downcast(Type, Type),
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
            Error::ArithmeticTypes(ref operation, ref left, ref right) => {
                write!(
                    f,
                    "Incompatible types for arithmetic operation {:?} ({:?}, {:?})",
                    operation,
                    left,
                    right
                )
            }
            Error::CombineSize(ref left, ref right) => {
                write!(
                    f,
                    "Combine arguments with different sizes: {} and {}",
                    left,
                    right
                )
            }
            Error::Downcast(ref expected, ref found) => {
                write!(
                    f,
                    "Downcast error: expected: {:?}, found {:?}",
                    expected,
                    found
                )
            }
            Error::PushType(ref col, ref val) => {
                write!(
                    f,
                    "Error pushing value {:?} to a column of type {:?}",
                    val,
                    col
                )
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


#[derive(Clone, Debug, Deserialize, Hash, PartialEq, Serialize)]
pub enum ArithmeticOp {
    Add,
    Subtract,
    Multiply,
    Divide,
}

impl ArithmeticOp {
    pub fn type_(&self, left: &Type, right: &Type) -> Result<Type> {
        match (self, left, right) {
            (&ArithmeticOp::Add, &Type::Int, &Type::Int) |
            (&ArithmeticOp::Subtract, &Type::Int, &Type::Int) |
            (&ArithmeticOp::Multiply, &Type::Int, &Type::Int) => Ok(Type::Int),
            (&ArithmeticOp::Add, &Type::Float, &Type::Float) |
            (&ArithmeticOp::Subtract, &Type::Float, &Type::Float) |
            (&ArithmeticOp::Multiply, &Type::Float, &Type::Float) |
            (&ArithmeticOp::Divide, &Type::Int, &Type::Int) |
            (&ArithmeticOp::Divide, &Type::Float, &Type::Float) => Ok(Type::Float),
            _ => Err(Error::ArithmeticTypes(
                self.clone(),
                left.clone(),
                right.clone(),
            )),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub enum AnyBlock {
    Bool(Vec<bool>),
    Int(Vec<i64>),
    Float(Vec<f64>),
    String(Vec<String>),
    BoolList(Vec<Vec<bool>>),
    IntList(Vec<Vec<i64>>),
    FloatList(Vec<Vec<f64>>),
    StringList(Vec<Vec<String>>),
}

impl AnyBlock {
    pub fn into_block(self) -> Box<Block> {
        match self {
            AnyBlock::Bool(values) => box BoolBlock::new(values),
            AnyBlock::Int(values) => box IntBlock::new(values),
            AnyBlock::Float(values) => box FloatBlock::new(values),
            AnyBlock::String(values) => box StringBlock::new(values),
            AnyBlock::BoolList(values) => box ListBlock::Bool(values),
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
    [Bool, Vec<bool>],
    [Int, Vec<i64>],
    [Float, Vec<f64>],
    [String, Vec<String>],
    [BoolList, Vec<Vec<bool>>],
    [IntList, Vec<Vec<i64>>],
    [FloatList, Vec<Vec<f64>>],
    [StringList, Vec<Vec<String>>]
);

impl<'a> From<Vec<&'a str>> for AnyBlock {
    fn from(values: Vec<&'a str>) -> AnyBlock {
        AnyBlock::String(values.into_iter().map(|s| s.to_string()).collect())
    }
}

pub trait Block: fmt::Debug + Downcast + Send + Sync {
    fn type_(&self) -> Type;
    fn len(&self) -> usize;

    fn cmp_at_indices(&self, usize, usize) -> cmp::Ordering;
    fn select_by_idx(&self, &[usize]) -> Box<Block>;

    fn get(&self, usize) -> Value;

    fn filter(&self, &Predicate) -> Result<(Vec<usize>, Box<Block>)>;
    fn order_by(&self, &[usize]) -> Box<Block>;
    fn group_by(&self, &[usize]) -> Box<Block>;
    fn aggregate(&self, &Aggregator) -> Result<Box<Block>>;

    fn combine(&self, &Block, &ArithmeticOp) -> Result<Box<Block>>;
    fn union(&mut self, &mut Block) -> Result<()>;

    fn into_any_block(&self) -> AnyBlock;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl_downcast!(Block);

pub trait BlockBuilder: fmt::Debug {
    fn push(&mut self, Value) -> Result<()>;
    fn build(self: Box<Self>) -> Box<Block>;
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

fn gen_order_by<T: Clone + Nullable + PartialOrd>(values: &[T], sort_order: &[usize]) -> Vec<T> {
    let mut buffer = values
        .iter()
        .enumerate()
        .map(|(idx, v)| (sort_order[idx], v))
        .collect::<Vec<(usize, &T)>>();
    buffer.sort_by(|&(left_order, _), &(right_order, _)| {
        left_order.cmp(&right_order)
    });
    buffer.into_iter().map(|(_, value)| value.clone()).collect()
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
struct BoolBlock {
    values: Vec<bool>,
}

impl BoolBlock {
    fn new(values: Vec<bool>) -> Self {
        BoolBlock { values }
    }
}

impl Block for BoolBlock {
    fn type_(&self) -> Type {
        Type::Int
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn cmp_at_indices(&self, left: usize, right: usize) -> cmp::Ordering {
        self.values[left].cmp(&self.values[right])
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        box BoolBlock::new(gen_select_by_idx(&self.values, indices))
    }

    fn get(&self, idx: usize) -> Value {
        Value::from(self.values[idx])
    }

    fn filter(&self, predicate: &Predicate) -> Result<(Vec<usize>, Box<Block>)> {
        if let Value::Bool(ref value) = predicate.value {
            let (indices, filtered) = gen_filter(&predicate.comparator, &self.values, value);
            Ok((indices, box BoolBlock::new(filtered)))
        } else {
            Err(Error::PredicateAndValueTypes(
                predicate.value.type_(),
                self.type_(),
            ))
        }
    }

    fn order_by(&self, sort_order: &[usize]) -> Box<Block> {
        box BoolBlock::new(gen_order_by(&self.values, sort_order))
    }

    fn group_by(&self, group_offsets: &[usize]) -> Box<Block> {
        box ListBlock::Bool(gen_group_by(&self.values, group_offsets))
    }

    fn aggregate(&self, aggregator: &Aggregator) -> Result<Box<Block>> {
        match *aggregator {
            Aggregator::Count => Ok(box IntBlock::new(vec![self.len() as i64])),
            Aggregator::First => Ok(box BoolBlock::new(vec![
                Aggregator::first(&self.values)?,
            ])),
            Aggregator::Max => Ok(box BoolBlock::new(vec![Aggregator::max(&self.values)?])),
            Aggregator::Min => Ok(box BoolBlock::new(vec![Aggregator::min(&self.values)?])),
            Aggregator::Average | Aggregator::Sum => Err(Error::from(
                aggregate::Error::AggregatorAndColumnType(
                    aggregator.clone(),
                    self.type_(),
                ),
            )),
        }
    }

    fn union(&mut self, other: &mut Block) -> Result<()> {
        let other_type = other.type_();
        let downcasted = other.downcast_mut::<BoolBlock>().ok_or_else(|| {
            Error::Downcast(Type::Int, other_type)
        })?;
        self.values.append(&mut downcasted.values);
        Ok(())
    }

    fn combine(&self, _other: &Block, _operation: &ArithmeticOp) -> Result<Box<Block>> {
        unimplemented!()
    }

    fn into_any_block(&self) -> AnyBlock {
        AnyBlock::Bool(self.values.clone())
    }
}

#[derive(Debug, Default)]
struct BoolBlockBuilder {
    values: Vec<bool>,
}

impl BlockBuilder for BoolBlockBuilder {
    fn push(&mut self, value: Value) -> Result<()> {
        match value {
            Value::Bool(v) => self.values.push(v),
            _ => return Err(Error::PushType(Type::Bool, value)),
        }
        Ok(())
    }

    #[allow(boxed_local)]
    fn build(self: Box<Self>) -> Box<Block> {
        box BoolBlock::new(self.values)
    }
}

#[derive(Debug, Clone)]
struct IntBlock {
    values: Vec<i64>,
}

impl IntBlock {
    pub fn new(values: Vec<i64>) -> Self {
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

    fn cmp_at_indices(&self, left: usize, right: usize) -> cmp::Ordering {
        self.values[left].cmp(&self.values[right])
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        box IntBlock::new(gen_select_by_idx(&self.values, indices))
    }

    fn get(&self, idx: usize) -> Value {
        Value::from(self.values[idx])
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

    fn order_by(&self, sort_order: &[usize]) -> Box<Block> {
        box IntBlock::new(gen_order_by(&self.values, sort_order))
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

    fn union(&mut self, other: &mut Block) -> Result<()> {
        let other_type = other.type_();
        let downcasted = other.downcast_mut::<IntBlock>().ok_or_else(|| {
            Error::Downcast(Type::Int, other_type)
        })?;
        self.values.append(&mut downcasted.values);
        Ok(())
    }

    fn combine(&self, other: &Block, operation: &ArithmeticOp) -> Result<Box<Block>> {
        if self.len() != other.len() {
            return Err(Error::CombineSize(self.len(), other.len()));
        }
        let downcasted = other.downcast_ref::<IntBlock>().ok_or_else(|| {
            Error::Downcast(Type::Int, other.type_())
        })?;

        let zipped = self.values.iter().zip(downcasted.values.iter());

        if *operation == ArithmeticOp::Divide {
            Ok(box FloatBlock::new(
                zipped
                    .map(|(left, right)| *left as f64 / *right as f64)
                    .collect(),
            ))
        } else {
            Ok(box IntBlock::new(
                zipped
                    .map(|(left, right)| match *operation {
                        ArithmeticOp::Add => left + right,
                        ArithmeticOp::Subtract => left - right,
                        ArithmeticOp::Multiply => left * right,
                        ArithmeticOp::Divide => unreachable!(),
                    })
                    .collect(),
            ))
        }
    }

    fn into_any_block(&self) -> AnyBlock {
        AnyBlock::Int(self.values.clone())
    }
}

#[derive(Debug, Default)]
struct IntBlockBuilder {
    values: Vec<i64>,
}

impl BlockBuilder for IntBlockBuilder {
    fn push(&mut self, value: Value) -> Result<()> {
        match value {
            Value::Int(v) => self.values.push(v),
            _ => return Err(Error::PushType(Type::Int, value)),
        }
        Ok(())
    }

    #[allow(boxed_local)]
    fn build(self: Box<Self>) -> Box<Block> {
        box IntBlock::new(self.values)
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

    fn cmp_at_indices(&self, left: usize, right: usize) -> cmp::Ordering {
        nullable_partial_cmp(&self.values[left], &self.values[right])
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        box FloatBlock::new(gen_select_by_idx(&self.values, indices))
    }

    fn get(&self, idx: usize) -> Value {
        Value::from(self.values[idx])
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

    fn order_by(&self, sort_order: &[usize]) -> Box<Block> {
        box FloatBlock::new(gen_order_by(&self.values, sort_order))
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

    fn union(&mut self, other: &mut Block) -> Result<()> {
        let other_type = other.type_();
        let downcasted = other.downcast_mut::<FloatBlock>().ok_or_else(|| {
            Error::Downcast(Type::Int, other_type)
        })?;
        self.values.append(&mut downcasted.values);
        Ok(())
    }

    fn combine(&self, other: &Block, operation: &ArithmeticOp) -> Result<Box<Block>> {
        if self.len() != other.len() {
            return Err(Error::CombineSize(self.len(), other.len()));
        }
        let downcasted = other.downcast_ref::<FloatBlock>().ok_or_else(|| {
            Error::Downcast(Type::Float, other.type_())
        })?;

        Ok(box FloatBlock::new(
            self.values
                .iter()
                .zip(downcasted.values.iter())
                .map(|(left, right)| match *operation {
                    ArithmeticOp::Add => left + right,
                    ArithmeticOp::Subtract => left - right,
                    ArithmeticOp::Multiply => left * right,
                    ArithmeticOp::Divide => left / right,
                })
                .collect(),
        ))
    }

    fn into_any_block(&self) -> AnyBlock {
        AnyBlock::Float(self.values.clone())
    }
}

#[derive(Debug, Default)]
struct FloatBlockBuilder {
    values: Vec<f64>,
}

impl BlockBuilder for FloatBlockBuilder {
    fn push(&mut self, value: Value) -> Result<()> {
        match value {
            Value::Float(v) => self.values.push(v.into_inner()),
            _ => return Err(Error::PushType(Type::Float, value)),
        }
        Ok(())
    }

    #[allow(boxed_local)]
    fn build(self: Box<Self>) -> Box<Block> {
        box FloatBlock::new(self.values)
    }
}

const STRING_CHUNK_SIZE: usize = 1_000_000_000;

#[derive(Clone, Debug)]
struct StringBlock {
    values: Vec<String>,
    indices: Vec<(usize, usize)>,
}

impl StringBlock {
    fn new(values: Vec<String>) -> Self {
        let mut builder = box StringBlockBuilder::default();
        for value in values {
            builder.push_primitive(&value)
        }
        builder.build_primitive()
    }

    fn from_slices(values: Vec<&str>) -> Self {
        let mut builder = box StringBlockBuilder::default();
        for value in values {
            builder.push_primitive(value)
        }
        builder.build_primitive()
    }

    fn from_chunked(values: Vec<String>, indices: Vec<(usize, usize)>) -> Self {
        StringBlock { values, indices }
    }

    fn get_primitive(&self, idx: &usize) -> Option<&str> {
        if idx >= &self.indices.len() {
            None
        } else {
            let (chunk_idx, str_start) = self.indices[*idx];
            let chunk = &self.values[chunk_idx];
            let str_end =
                if *idx != self.indices.len() - 1 && self.indices[idx + 1].0 == chunk_idx {
                    self.indices[idx + 1].1
                } else {
                    chunk.len()
                };
            Some(&chunk[str_start..str_end])
        }
    }

    fn iter(&self) -> StringBlockIter {
        StringBlockIter {
            block: self,
            idx: 0,
        }
    }
}

impl Block for StringBlock {
    fn type_(&self) -> Type {
        Type::String
    }

    fn len(&self) -> usize {
        self.indices.len()
    }

    fn cmp_at_indices(&self, left: usize, right: usize) -> cmp::Ordering {
        self.get_primitive(&left).cmp(&self.get_primitive(&right))
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        let mut builder = box StringBlockBuilder::default();
        for idx in indices {
            builder.push_primitive(self.get_primitive(idx).unwrap())
        }
        builder.build()
    }

    fn get(&self, idx: usize) -> Value {
        Value::from(self.get_primitive(&idx).unwrap())
    }

    fn filter(&self, predicate: &Predicate) -> Result<(Vec<usize>, Box<Block>)> {
        if let Value::String(ref pred_value) = predicate.value {
            let mut builder = box StringBlockBuilder::default();
            let mut indices = vec![];
            for (idx, value) in self.iter().enumerate() {
                if predicate.comparator.pass(&value, &pred_value.as_str()) {
                    indices.push(idx);
                    builder.push_primitive(value)
                }
            }
            Ok((indices, builder.build()))
        } else {
            Err(Error::PredicateAndValueTypes(
                predicate.value.type_(),
                self.type_(),
            ))
        }
    }

    fn order_by(&self, sort_order: &[usize]) -> Box<Block> {
        let values = &self.iter().collect::<Vec<&str>>();
        box StringBlock::from_slices(gen_order_by(values, sort_order))
    }

    fn group_by(&self, group_offsets: &[usize]) -> Box<Block> {
        let values = &self.iter().collect::<Vec<&str>>();
        let grouped = gen_group_by(values, group_offsets);
        box ListBlock::String(
            grouped
                .into_iter()
                .map(|vs| vs.into_iter().map(|v| v.to_string()).collect())
                .collect(),
        )
    }

    fn aggregate(&self, aggregator: &Aggregator) -> Result<Box<Block>> {
        match *aggregator {
            Aggregator::Count => Ok(box IntBlock::new(vec![self.indices.len() as i64])),
            Aggregator::First => {
                if self.is_empty() {
                    Err(Error::from(aggregate::Error::EmptyColumn))
                } else {
                    Ok(box StringBlock::new(vec![
                        self.get_primitive(&0).unwrap().to_string(),
                    ]))
                }
            }
            Aggregator::Max | Aggregator::Min | Aggregator::Average | Aggregator::Sum => {
                Err(Error::from(aggregate::Error::AggregatorAndColumnType(
                    aggregator.clone(),
                    self.type_(),
                )))
            }
        }
    }

    fn union(&mut self, other: &mut Block) -> Result<()> {
        let other_type = other.type_();
        let downcasted = other.downcast_mut::<StringBlock>().ok_or_else(|| {
            Error::Downcast(Type::Int, other_type)
        })?;
        self.values.append(&mut downcasted.values);
        let mut incremented_indices = downcasted
            .indices
            .iter()
            .map(|&(chunk_idx, str_idx)| {
                (chunk_idx + self.indices.last().unwrap().0 + 1, str_idx)
            })
            .collect();
        self.indices.append(&mut incremented_indices);
        Ok(())
    }

    fn combine(&self, _other: &Block, _operation: &ArithmeticOp) -> Result<Box<Block>> {
        unimplemented!()
    }

    fn into_any_block(&self) -> AnyBlock {
        AnyBlock::String(self.iter().map(|s| s.to_string()).collect())
    }
}

#[derive(Debug, Default)]
struct StringBlockBuilder {
    values: Vec<String>,
    indices: Vec<(usize, usize)>,
}

impl StringBlockBuilder {
    fn push_primitive(&mut self, value: &str) -> () {
        if self.values.is_empty() {
            self.values.push(String::new())
        }
        let mut current_len = self.values.last().unwrap().len();
        if current_len + value.len() > STRING_CHUNK_SIZE {
            self.values.push(String::new());
            current_len = 0;
        }

        self.indices.push((self.values.len() - 1, current_len));
        self.values.last_mut().unwrap().push_str(value);
    }

    fn build_primitive(mut self) -> StringBlock {
        let indices = mem::replace(&mut self.indices, vec![]);
        StringBlock::from_chunked(self.values, indices)
    }
}

impl BlockBuilder for StringBlockBuilder {
    fn push(&mut self, value: Value) -> Result<()> {
        match value {
            Value::String(v) => self.push_primitive(&v),
            _ => return Err(Error::PushType(Type::String, value)),
        }
        Ok(())
    }

    #[allow(boxed_local)]
    fn build(self: Box<Self>) -> Box<Block> {
        box self.build_primitive()
    }
}

pub fn builder(type_: &Type) -> Box<BlockBuilder> {
    match *type_ {
        Type::Bool => box BoolBlockBuilder::default(),
        Type::Int => box IntBlockBuilder::default(),
        Type::Float => box FloatBlockBuilder::default(),
        Type::String => box StringBlockBuilder::default(),
        _ => unimplemented!(),
    }
}

pub fn builders(types: &[&Type]) -> Vec<Box<BlockBuilder>> {
    let mut builders: Vec<Box<BlockBuilder>> = vec![];
    for type_ in types.iter() {
        builders.push(builder(type_))
    }
    builders
}

struct StringBlockIter<'a> {
    block: &'a StringBlock,
    idx: usize,
}

impl<'a> Iterator for StringBlockIter<'a> {
    type Item = &'a str;
    fn next(&mut self) -> Option<Self::Item> {
        let result = self.block.get_primitive(&self.idx);
        if result.is_some() {
            self.idx += 1
        }
        result
    }
}

#[derive(Clone, Debug)]
enum ListBlock {
    Bool(Vec<Vec<bool>>),
    Int(Vec<Vec<i64>>),
    Float(Vec<Vec<f64>>),
    String(Vec<Vec<String>>),
}

impl ListBlock {
    fn average(&self) -> Result<Box<Block>> {
        match *self {
            ListBlock::Bool(_) => Err(Error::from(aggregate::Error::AggregatorAndColumnType(
                Aggregator::Average,
                Type::Bool,
            ))),
            ListBlock::Int(ref values) => Ok(box FloatBlock::new(
                values
                    .iter()
                    .map(|vs| vs.iter().sum::<i64>() as f64 / vs.len() as f64)
                    .collect(),
            )),
            ListBlock::Float(ref values) => Ok(box FloatBlock::new(
                values
                    .iter()
                    .map(|vs| vs.iter().sum::<f64>() as f64 / vs.len() as f64)
                    .collect(),
            )),
            ListBlock::String(_) => Err(Error::from(aggregate::Error::AggregatorAndColumnType(
                Aggregator::Average,
                Type::String,
            ))),
        }
    }

    fn count(&self) -> Result<Box<Block>> {
        match *self {
            ListBlock::Bool(ref values) => Ok(box IntBlock::new(
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
            ListBlock::Bool(_) => Err(Error::from(aggregate::Error::AggregatorAndColumnType(
                Aggregator::Sum,
                Type::Bool,
            ))),
            ListBlock::Int(ref values) => Ok(box IntBlock::new(
                values.iter().map(|vs| vs.iter().sum()).collect(),
            )),
            ListBlock::Float(ref values) => Ok(box FloatBlock::new(
                values.iter().map(|vs| vs.iter().sum()).collect(),
            )),
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
            ListBlock::Bool(ref values) => Ok(box BoolBlock::new(values
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
            ListBlock::Bool(_) => Type::List(box Type::Bool),
            ListBlock::Int(_) => Type::List(box Type::Int),
            ListBlock::Float(_) => Type::List(box Type::Float),
            ListBlock::String(_) => Type::List(box Type::String),
        }
    }

    fn len(&self) -> usize {
        match *self {
            ListBlock::Bool(ref values) => values.len(),
            ListBlock::Int(ref values) => values.len(),
            ListBlock::Float(ref values) => values.len(),
            ListBlock::String(ref values) => values.len(),
        }
    }

    fn cmp_at_indices(&self, _left: usize, _right: usize) -> cmp::Ordering {
        unimplemented!()
    }

    fn select_by_idx(&self, indices: &[usize]) -> Box<Block> {
        match *self {
            ListBlock::Bool(ref values) => box ListBlock::Bool(gen_select_by_idx(values, indices)),
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
            ListBlock::Bool(ref values) => Value::from(values[idx].clone()),
            ListBlock::Int(ref values) => Value::from(values[idx].clone()),
            ListBlock::Float(ref values) => Value::from(values[idx].clone()),
            ListBlock::String(ref values) => Value::from(values[idx].clone()),
        }
    }

    fn filter(&self, _predicate: &Predicate) -> Result<(Vec<usize>, Box<Block>)> {
        unimplemented!()
    }

    fn order_by(&self, _sort_order: &[usize]) -> Box<Block> {
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

    fn union(&mut self, _other: &mut Block) -> Result<()> {
        unimplemented!()
    }

    fn combine(&self, _other: &Block, _operation: &ArithmeticOp) -> Result<Box<Block>> {
        unimplemented!()
    }

    fn into_any_block(&self) -> AnyBlock {
        match *self {
            ListBlock::Bool(ref values) => AnyBlock::BoolList(values.clone()),
            ListBlock::Int(ref values) => AnyBlock::IntList(values.clone()),
            ListBlock::Float(ref values) => AnyBlock::FloatList(values.clone()),
            ListBlock::String(ref values) => AnyBlock::StringList(values.clone()),
        }
    }
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
