use std::cmp::Ordering;
use std::fmt;
use std::num;
use std::str;

use fnv::FnvHashMap;

use value::{Comparator, Predicate, Nullable, Type, Value};

#[derive(Debug)]
pub enum Error {
    ParseError(Type, String),
    PredicateAndValueTypes(Type, Type),
}

impl From<str::ParseBoolError> for Error {
    fn from(error: str::ParseBoolError) -> Error {
        Error::ParseError(Type::Boolean, format!("{:?}", error))
    }
}

impl From<num::ParseIntError> for Error {
    fn from(error: num::ParseIntError) -> Error {
        Error::ParseError(Type::Int, format!("{:?}", error))
    }
}

impl From<num::ParseFloatError> for Error {
    fn from(error: num::ParseFloatError) -> Error {
        Error::ParseError(Type::Float, format!("{:?}", error))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::ParseError(ref type_, ref message) => {
                write!(f, "Error parsing value of type {:?}: {}", type_, message)
            }
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

type Result<T> = ::std::result::Result<T, Error>;

#[allow(dead_code)]
type BlockRef<T> = Box<Block<Item = T>>;

type SortScores = FnvHashMap<usize, usize>;

trait Block {
    type Item;

    fn type_(&self) -> Type;
    fn len(&self) -> usize;
    fn equal_at_idxs(&self, usize, usize) -> bool;
    fn select_by_idx(&self, &[usize]) -> BlockRef<Self::Item>;
    fn filter(&self, &Predicate) -> Result<(Vec<usize>, BlockRef<Self::Item>)>;
    fn order_by(&self, &Option<SortScores>, bool) -> (SortScores, BlockRef<Self::Item>);
    // fn group_by(&self, &[usize]) -> BlockRef<Self::Item>;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn gen_select_by_idx<T: Copy>(values: &[T], indices: &[usize]) -> Vec<T> {
    values
        .iter()
        .enumerate()
        .filter(|&(k, _)| indices.contains(&k))
        .map(|(_, v)| *v)
        .collect()
}

fn gen_filter<T: Copy + PartialEq + PartialOrd>(
    comparator: &Comparator,
    values: &[T],
    value: &T,
) -> (Vec<usize>, Vec<T>) {
    let filtered = values
        .iter()
        .enumerate()
        .filter(|&(_, v)| comparator.pass(v, value))
        .map(|(k, v)| (k, *v))
        .collect::<Vec<(usize, T)>>();
    filtered.into_iter().unzip()
}

fn nullable_partial_cmp<T: Nullable + PartialOrd>(left: &T, right: &T) -> Ordering {
    left.partial_cmp(right).unwrap_or_else(
        || if left.is_null() {
            Ordering::Less
        } else {
            Ordering::Greater
        },
    )
}

fn gen_order_by<T: Copy + Nullable + PartialOrd>(
    values: &[T],
    sort_scores: &Option<SortScores>,
    only_use_score: bool,
) -> (SortScores, Vec<T>) {
    if values.is_empty() {
        return (FnvHashMap::default(), vec![]);
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

    let mut new_scores = FnvHashMap::default();
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
        sorted.into_iter().map(|(_, v)| *v).collect(),
    )
}

fn gen_group_by<T: Copy>(values: &[T], group_offsets: &[usize]) -> Vec<Vec<T>> {
    let mut offset_index = 0;
    let mut outer = vec![];
    let mut inner = vec![];
    for (idx, value) in values.iter().enumerate() {
        inner.push(*value);
        if idx == group_offsets[offset_index] {
            outer.push(inner);
            inner = vec![];
            offset_index += 1;
        }
    }
    outer
}

#[derive(Debug, Clone)]
struct BooleanBlock {
    values: Vec<bool>,
}

impl BooleanBlock {
    fn new(values: Vec<bool>) -> Self {
        BooleanBlock { values }
    }
}

impl Block for BooleanBlock {
    type Item = bool;

    fn type_(&self) -> Type {
        Type::Int
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn equal_at_idxs(&self, left: usize, right: usize) -> bool {
        self.values[left] == self.values[right]
    }

    fn select_by_idx(&self, indices: &[usize]) -> BlockRef<Self::Item> {
        box BooleanBlock::new(gen_select_by_idx(&self.values, indices))
    }

    fn filter(&self, predicate: &Predicate) -> Result<(Vec<usize>, BlockRef<Self::Item>)> {
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
        sort_scores: &Option<FnvHashMap<usize, usize>>,
        only_use_scores: bool,
    ) -> (SortScores, BlockRef<Self::Item>) {
        let (indices, ordered) = gen_order_by(&self.values, sort_scores, only_use_scores);
        (indices, box BooleanBlock::new(ordered))
    }

    // fn group_by(&self, group_offsets: &[usize]) -> BlockRef<Self::Item> {
    //     box BooleanBlock::new(gen_group_by(&self.values, group_offsets))
    // }
}

#[derive(Debug, Clone)]
struct IntBlock {
    max: i64,
    min: i64,
    values: Vec<i64>,
}

impl IntBlock {
    fn new(values: Vec<i64>) -> Self {
        IntBlock {
            max: *values.iter().max().unwrap(),
            min: *values.iter().min().unwrap(),
            values,
        }
    }
}

impl Block for IntBlock {
    type Item = i64;

    fn type_(&self) -> Type {
        Type::Int
    }

    fn len(&self) -> usize {
        self.values.len()
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn equal_at_idxs(&self, left: usize, right: usize) -> bool {
        self.values[left] == self.values[right]
    }

    fn select_by_idx(&self, indices: &[usize]) -> BlockRef<Self::Item> {
        box IntBlock::new(gen_select_by_idx(&self.values, indices))
    }

    fn filter(&self, predicate: &Predicate) -> Result<(Vec<usize>, BlockRef<Self::Item>)> {
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
        sort_scores: &Option<FnvHashMap<usize, usize>>,
        only_use_scores: bool,
    ) -> (SortScores, BlockRef<Self::Item>) {
        let (indices, ordered) = gen_order_by(&self.values, sort_scores, only_use_scores);
        (indices, box IntBlock::new(ordered))
    }

    // fn group_by(&self, group_offsets: &[usize]) -> BlockRef<Self::Item> {
    //     box IntBlock::new(gen_group_by(&self.values, group_offsets))
    // }
}
