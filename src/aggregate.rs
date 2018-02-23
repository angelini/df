use std::cmp::Ordering;
use std::fmt;

use value::{Nullable, Type};

#[derive(Debug)]
pub enum Error {
    AggregatorAndColumnType(Aggregator, Type),
    EmptyColumn,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::AggregatorAndColumnType(ref agg, ref type_) => {
                write!(f, "Aggregator {:?} cannot be applied to a column of type {:?}", agg, type_)
            }
            Error::EmptyColumn => {
                write!(f, "Aggregator cannot be applied to an empty column")
            }
        }
    }
}

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Clone, Debug, Deserialize, Hash, Serialize)]
pub enum Aggregator {
    Average,
    Count,
    First,
    Max,
    Min,
    Sum,
}

impl Aggregator {
    pub fn first<T: Clone>(values: &[T]) -> Result<T> {
        match values.first() {
            Some(v) => Ok(v.clone()),
            None => Err(Error::EmptyColumn),
        }
    }

    pub fn max<T: Clone + Nullable + PartialOrd>(values: &[T]) -> Result<T> {
        let first = match values.first() {
            Some(v) => v,
            None => return Err(Error::EmptyColumn),
        };
        Ok(
            values
                .iter()
                .skip(1)
                .fold(first, |acc, v| match acc.partial_cmp(v) {
                    Some(Ordering::Equal) |
                    Some(Ordering::Greater) => acc,
                    Some(Ordering::Less) => v,
                    None => if acc.is_null() { v } else { acc },
                })
                .clone(),
        )
    }

    pub fn min<T: Clone + Nullable + PartialOrd>(values: &[T]) -> Result<T> {
        let first = match values.first() {
            Some(v) => v,
            None => return Err(Error::EmptyColumn),
        };
        Ok(
            values
                .iter()
                .skip(1)
                .fold(first, |acc, v| match acc.partial_cmp(v) {
                    Some(Ordering::Equal) |
                    Some(Ordering::Less) => acc,
                    Some(Ordering::Greater) => v,
                    None => if acc.is_null() { v } else { acc },
                })
                .clone(),
        )
    }

    pub fn output_type(&self, input_type: &Type) -> Result<Type> {
        match *input_type {
            Type::List(box ref inner) => self.output_type(inner),
            _ => {
                match *self {
                    Aggregator::Average => {
                        if [Type::Int, Type::Float].contains(input_type) {
                            Ok(Type::Float)
                        } else {
                            Err(Error::AggregatorAndColumnType(self.clone(), input_type.clone()))
                        }
                    }
                    Aggregator::Count => Ok(Type::Int),
                    Aggregator::Sum => {
                        if [Type::Int, Type::Float].contains(input_type) {
                            Ok(input_type.clone())
                        } else {
                            Err(Error::AggregatorAndColumnType(self.clone(), input_type.clone()))
                        }
                    }
                    Aggregator::First | Aggregator::Max | Aggregator::Min => Ok(input_type.clone()),
                }
            }
        }
    }
}
