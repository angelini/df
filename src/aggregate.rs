use std::cmp::Ordering;
use std::fmt;

use value::{ListValues, Nullable, Type, Value, Values};

#[derive(Debug)]
pub enum Error {
    AverageOnInvalidType(Type),
    EmptyColumn,
    SumOnInvalidType(Type),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::AverageOnInvalidType(ref type_) => {
                write!(f, "Aggregate aggregate on {:?} column", type_)
            }
            Error::EmptyColumn => write!(f, "Aggregate on empty column"),
            Error::SumOnInvalidType(ref type_) => write!(f, "Sum aggregate on {:?} column", type_),
        }
    }
}

type Result<T> = ::std::result::Result<T, Error>;

macro_rules! simple_aggregate {
    ( $i:expr, $f:ident, $( $t:ident, $pt:ty ),* ) => {
        match *$i {
            Values::List(ref list_values) => {
                match *list_values {
                    $(
                        ListValues::$t(ref values) => {
                            Values::from(values.iter()
                                    .map(|vs| Aggregator::$f(vs))
                                    .collect::<Result<Vec<$pt>>>()?
                            )
                        }
                    )*
                }
            }
            $(
                Values::$t(ref values) => Values::from(Value::from(Aggregator::$f(values)?)),
            )*
        }
    };
}

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
    pub fn output_type(&self, input_type: &Type) -> Result<Type> {
        match *input_type {
            Type::List(box ref inner) => self.output_type(inner),
            _ => {
                match *self {
                    Aggregator::Average => {
                        if [Type::Int, Type::Float].contains(input_type) {
                            Ok(Type::Float)
                        } else {
                            Err(Error::SumOnInvalidType(input_type.clone()))
                        }
                    }
                    Aggregator::Count => Ok(Type::Int),
                    Aggregator::Sum => {
                        if [Type::Int, Type::Float].contains(input_type) {
                            Ok(input_type.clone())
                        } else {
                            Err(Error::SumOnInvalidType(input_type.clone()))
                        }
                    }
                    Aggregator::First | Aggregator::Max | Aggregator::Min => Ok(input_type.clone()),
                }
            }
        }
    }

    pub fn aggregate(&self, input: &Values) -> Result<Values> {
        Ok(match *self {
            Aggregator::Average => {
                match *input {
                    Values::List(ref list_values) => {
                        match *list_values {
                            ListValues::Int(ref values) => {
                                Values::from(
                                    values
                                        .iter()
                                        .map(|vs| vs.iter().sum::<i64>() as f64 / vs.len() as f64)
                                        .collect::<Vec<f64>>(),
                                )
                            }
                            ListValues::Float(ref values) => {
                                Values::from(
                                    values
                                        .iter()
                                        .map(|vs| {
                                            vs.iter().fold(0.0, |acc, &v| acc + v) / vs.len() as f64
                                        })
                                        .collect::<Vec<f64>>(),
                                )
                            }
                            _ => return Err(Error::AverageOnInvalidType(input.type_())),
                        }
                    }
                    Values::Int(ref values) => Values::from(vec![
                        values.iter().sum::<i64>() as f64 /
                            values.len() as f64,
                    ]),
                    Values::Float(ref values) => Values::from(vec![
                        values.iter().fold(0.0, |acc, &v| acc + v) /
                            values.len() as f64,
                    ]),
                    _ => return Err(Error::AverageOnInvalidType(input.type_())),
                }
            }
            Aggregator::Count => {
                simple_aggregate!(
                    input,
                    count,
                    Boolean,
                    i64,
                    Int,
                    i64,
                    Float,
                    i64,
                    String,
                    i64
                )
            }
            Aggregator::First => {
                simple_aggregate!(
                    input,
                    first,
                    Boolean,
                    bool,
                    Int,
                    i64,
                    Float,
                    f64,
                    String,
                    String
                )
            }
            Aggregator::Max => {
                simple_aggregate!(
                    input,
                    max,
                    Boolean,
                    bool,
                    Int,
                    i64,
                    Float,
                    f64,
                    String,
                    String
                )
            }
            Aggregator::Min => {
                simple_aggregate!(
                    input,
                    min,
                    Boolean,
                    bool,
                    Int,
                    i64,
                    Float,
                    f64,
                    String,
                    String
                )
            }
            Aggregator::Sum => {
                match *input {
                    Values::List(ref list_values) => {
                        match *list_values {
                            ListValues::Int(ref values) => {
                                Values::from(
                                    values
                                        .iter()
                                        .map(|vs| vs.iter().sum())
                                        .collect::<Vec<i64>>(),
                                )
                            }
                            ListValues::Float(ref values) => {
                                Values::from(
                                    values
                                        .iter()
                                        .map(|vs| vs.iter().fold(0.0, |acc, &v| acc + v))
                                        .collect::<Vec<f64>>(),
                                )
                            }
                            _ => return Err(Error::SumOnInvalidType(input.type_())),
                        }
                    }
                    Values::Int(ref values) => Values::from(Value::Int(values.iter().sum())),
                    Values::Float(ref values) => Values::from(
                        vec![values.iter().fold(0.0, |acc, &v| acc + v)],
                    ),
                    _ => return Err(Error::SumOnInvalidType(input.type_())),
                }
            }
        })
    }

    fn count<T>(values: &[T]) -> Result<i64> {
        Ok(values.len() as i64)
    }

    fn first<T: Clone>(values: &[T]) -> Result<T> {
        match values.first() {
            Some(v) => Ok(v.clone()),
            None => Err(Error::EmptyColumn),
        }
    }

    fn max<T: Clone + Nullable + PartialOrd>(values: &[T]) -> Result<T> {
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

    fn min<T: Clone + Nullable + PartialOrd>(values: &[T]) -> Result<T> {
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
}
