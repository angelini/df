use std::rc::Rc;

#[derive(Clone, Debug, PartialEq)]
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

#[derive(Clone, Debug)]
pub enum Values {
    Boolean(Rc<Vec<bool>>),
    Int(Rc<Vec<u64>>),
    String(Rc<Vec<String>>),
}

impl Values {
    pub fn filter(&self, predicate: &Predicate) -> (Vec<usize>, Values) {
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
                    Values::from(filtered.into_iter().map(|(_, v)| v).collect::<Vec<bool>>()),
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
                    Values::from(filtered.into_iter().map(|(_, v)| v).collect::<Vec<u64>>()),
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

    pub fn select_by_idx(&self, indices: &[usize]) -> Values {
        match *self {
            Values::Boolean(ref values) => Values::from(Self::gen_select_by_idx(values, indices)),
            Values::Int(ref values) => Values::from(Self::gen_select_by_idx(values, indices)),
            Values::String(ref values) => Values::from(Self::gen_select_by_idx(values, indices)),
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
            Values::String(ref values) => {
                let (indices, sorted_values) =
                    Self::gen_sort(values, parent_sorting, only_use_parent);
                (indices, Values::from(sorted_values))
            }
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

impl From<Vec<String>> for Values {
    fn from(values: Vec<String>) -> Self {
        Values::String(Rc::new(values))
    }
}

impl From<Value> for Values {
    fn from(value: Value) -> Self {
        match value {
            Value::Boolean(value) => Values::Boolean(Rc::new(vec![value])),
            Value::Int(value) => Values::Int(Rc::new(vec![value])),
            Value::String(value) => Values::String(Rc::new(vec![value])),
        }
    }
}
