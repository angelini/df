use decorum::R64;
use std::collections::HashMap;
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

#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum Value {
    Boolean(bool),
    Int(u64),
    Float(R64),
    String(String),
    BooleanList(Vec<bool>),
    IntList(Vec<u64>),
    FloatList(Vec<R64>),
    StringList(Vec<String>),
}

macro_rules! value_type {
    ( $v:expr, $( $t:ident, $l:ident ),* ) => {
        match *$v {
            $(
                Value::$t(_) => Type::$t,
                Value::$l(_) => Type::List(box Type::$t),
            )*

        }
    };
}

impl Value {
    pub fn type_(&self) -> Type {
        value_type!(
            self,
            Boolean,
            BooleanList,
            Int,
            IntList,
            Float,
            FloatList,
            String,
            StringList
        )
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

impl From<Vec<bool>> for Value {
    fn from(value: Vec<bool>) -> Self {
        Value::BooleanList(value)
    }
}

impl From<Vec<u64>> for Value {
    fn from(value: Vec<u64>) -> Self {
        Value::IntList(value)
    }
}

impl From<Vec<R64>> for Value {
    fn from(value: Vec<R64>) -> Self {
        Value::FloatList(value)
    }
}

impl From<Vec<String>> for Value {
    fn from(value: Vec<String>) -> Self {
        Value::StringList(value)
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

macro_rules! list_select_by_idx {
    ( $v:expr, $i:expr, $( $t:ident ),* ) => {
        match *$v {
            $(
                ListValues::$t(ref values) => ListValues::$t(gen_select_by_idx(values, $i)),
            )*
        }
    };
}

macro_rules! len {
    ( $v:expr, $e:ident, $( $t:ident ),* ) => {
        match *$v {
            $(
                $e::$t(ref values) => values.len(),
            )*
        }
    };
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
        len!(self, ListValues, Boolean, Int, Float, String)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn select_by_idx(&self, indices: &[usize]) -> ListValues {
        list_select_by_idx!(self, indices, Boolean, Int, Float, String)
    }
}

macro_rules! select_by_idx {
    ( $v:expr, $i:expr, $l:ident, $( $t:ident ),* ) => {
        match *$v {
            Values::$l(ref values) => Values::from(values.select_by_idx($i)),
            $(
                Values::$t(ref values) => Values::from(gen_select_by_idx(values, $i)),
            )*
        }
    };
}

macro_rules! sort {
    ( $v:expr, $p:expr, $o:expr, $l:ident, $( $t:ident ),* ) => {
        match *$v {
            Values::$l(_) => unimplemented!(),
            $(
                Values::$t(ref values) => {
                    let (indices, sorted_values) = Values::gen_sort(values, $p, $o);
                    (indices, Values::from(sorted_values))
                }
            )*
        }
    };
}

macro_rules! group_by {
    ( $v:expr, $o:expr, $l:ident, $( $t:ident ),* ) => {
        match *$v {
            Values::$l(_) => unimplemented!(),
            $(
                Values::$t(ref values) => Values::from(ListValues::$t(Values::gen_group_by(values, $o))),
            )*
        }
    };
}

macro_rules! distinct {
    ( $v:expr, $l:ident, $( $t:ident ),* ) => {
        match *$v {
            Values::$l(_) => unimplemented!(),
            $(
                Values::$t(ref values) => Values::from(Values::gen_distinct(values)),
            )*
        }
    };
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

    pub fn len(&self) -> usize {
        len!(self, Values, List, Boolean, Int, Float, String)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn select_by_idx(&self, indices: &[usize]) -> Values {
        select_by_idx!(self, indices, List, Boolean, Int, Float, String)
    }

    pub fn sort(
        &self,
        sort_scores: &Option<HashMap<usize, usize>>,
        only_use_scores: bool,
    ) -> (HashMap<usize, usize>, Values) {
        sort!(
            self,
            sort_scores,
            only_use_scores,
            List,
            Boolean,
            Int,
            Float,
            String
        )
    }

    pub fn equal_at_idxs(&self, left: usize, right: usize) -> bool {
        match *self {
            Values::Boolean(ref values) => values[left] == values[right],
            Values::Int(ref values) => values[left] == values[right],
            Values::Float(ref values) => values[left] == values[right],
            Values::String(ref values) => values[left] == values[right],
            Values::List(_) => unimplemented!(),
        }
    }

    pub fn group_by(&self, group_offsets: &[usize]) -> Values {
        group_by!(self, group_offsets, List, Boolean, Int, Float, String)
    }

    pub fn distinct(&self) -> Values {
        distinct!(self, List, Boolean, Int, Float, String)
    }

    fn gen_sort<T: Clone + Ord>(
        values: &[T],
        sort_scores: &Option<HashMap<usize, usize>>,
        only_use_score: bool,
    ) -> (HashMap<usize, usize>, Vec<T>) {
        if values.is_empty() {
            return (HashMap::new(), vec![]);
        }
        let mut sorted = values.iter().enumerate().collect::<Vec<(usize, &T)>>();
        sorted.sort_by(|&(left_idx, left_value), &(right_idx, right_value)| {
            match *sort_scores {
                Some(ref sort_scores) => {
                    let left_score = sort_scores[&left_idx];
                    let right_score = sort_scores[&right_idx];
                    if left_score == right_score && !only_use_score {
                        left_value.cmp(right_value)
                    } else {
                        left_score.cmp(&right_score)
                    }
                }
                None => left_value.cmp(right_value),
            }
        });
        let mut new_scores = HashMap::new();
        new_scores.insert(sorted[0].0, 0);
        for (idx, &(row_idx, value)) in sorted.iter().enumerate().skip(1) {
            let previous_score = new_scores[&sorted[idx - 1].0];
            if value == sorted[idx - 1].1 {
                new_scores.insert(row_idx, previous_score);
            } else {
                new_scores.insert(row_idx, previous_score + 1);
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
            if offset_index < group_offsets.len() && idx == group_offsets[offset_index] {
                outer.push(inner);
                inner = vec![];
                offset_index += 1;
            }
            inner.push(value.clone())
        }
        outer.push(inner);
        outer
    }

    fn gen_distinct<T: Clone + PartialEq>(values: &[T]) -> Vec<T> {
        if values.is_empty() {
            return vec![];
        }
        let mut result = vec![values[0].clone()];
        for value in values.iter().skip(1) {
            if value != result.last().unwrap() {
                result.push(value.clone());
            }
        }
        result
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
            Value::BooleanList(value) => Values::List(Rc::new(ListValues::Boolean(vec![value]))),
            Value::IntList(value) => Values::List(Rc::new(ListValues::Int(vec![value]))),
            Value::FloatList(value) => Values::List(Rc::new(ListValues::Float(vec![value]))),
            Value::StringList(value) => Values::List(Rc::new(ListValues::String(vec![value]))),
        }
    }
}

#[derive(Clone, Debug, Hash)]
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

macro_rules! filter {
    ( $p:expr, $v:expr, $l:ident, $( $t:ident ),* ) => {
        match ($v, &$p.value) {
            $(
                (&Values::$t(ref values), &Value::$t(ref value)) => {
                    let (indices, filtered_values) = Predicate::gen_filter(&$p.comparator, values, value);
                    Ok((indices, Values::from(filtered_values)))
                }
            )*
            (&Values::$l(_), _) => unimplemented!(),
            (values, value) => Err(Error::PredicateAndValueTypes(values.type_(), value.type_())),
        }
    };
}

#[derive(Clone, Debug, Hash)]
pub struct Predicate {
    comparator: Comparator,
    value: Value,
}

impl Predicate {
    pub fn new(comparator: Comparator, value: Value) -> Predicate {
        Predicate { comparator, value }
    }

    pub fn filter(&self, values: &Values) -> Result<(Vec<usize>, Values)> {
        filter!(self, values, List, Boolean, Int, Float, String)
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
