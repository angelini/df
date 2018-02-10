use std::fmt;
use std::result;
use decorum::R64;

use value::{ListValues, Type, Value, Values};

#[derive(Debug)]
pub enum Error {
    EmptyColumn,
    SumOnInvalidType(Type),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::EmptyColumn => write!(f, "Aggregate on empty column"),
            Error::SumOnInvalidType(ref type_) => write!(f, "Sum aggregate on {:?} column", type_),
        }
    }
}

type Result<T> = result::Result<T, Error>;

macro_rules! simple_aggregate {
    ( $i:expr, $f:ident, $( $t:ident, $pt:ty ),* ) => {
        match $i {
            Values::List(list_values) => {
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
                Values::$t(values) => Values::from(Value::$t(Aggregator::$f(&values)?)),
            )*
        }
    };
}
#[derive(Clone, Debug, Hash)]
pub enum Aggregator {
    First,
    Sum,
    Max,
    Min,
}

impl Aggregator {
    pub fn output_type(&self, input_type: &Type) -> Result<Type> {
        match *input_type {
            Type::List(box ref inner) => self.output_type(inner),
            _ => {
                match *self {
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

    pub fn aggregate(&self, input: Values) -> Result<Values> {
        Ok(match *self {
            Aggregator::First => {
                simple_aggregate!(
                    input,
                    first,
                    Boolean,
                    bool,
                    Int,
                    u64,
                    Float,
                    R64,
                    String,
                    String
                )
            }
            Aggregator::Sum => {
                match input {
                    Values::List(ref list_values) => {
                        match **list_values {
                            ListValues::Int(ref values) => {
                                Values::from(
                                    values
                                        .iter()
                                        .map(|vs| vs.iter().sum())
                                        .collect::<Vec<u64>>(),
                                )
                            }
                            ListValues::Float(ref values) => {
                                Values::from(
                                    values
                                        .iter()
                                        .map(|vs| vs.iter().fold(R64::from_inner(0.0),
                                                                 |acc, &v| acc + v))
                                        .collect::<Vec<R64>>()
                                )
                            }
                            _ => return Err(Error::SumOnInvalidType(input.type_())),
                        }
                    }
                    Values::Int(values) => Values::from(Value::Int(values.iter().sum())),
                    Values::Float(values) => Values::from(Value::Float(values.iter().fold(
                        R64::from_inner(0.0),
                        |acc, &v| acc + v,
                    ))),
                    _ => return Err(Error::SumOnInvalidType(input.type_())),
                }
            }
            Aggregator::Max => {
                simple_aggregate!(
                    input,
                    max,
                    Boolean,
                    bool,
                    Int,
                    u64,
                    Float,
                    R64,
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
                    u64,
                    Float,
                    R64,
                    String,
                    String
                )
            }
        })
    }

    fn first<T: Clone>(values: &[T]) -> Result<T> {
        match values.first() {
            Some(v) => Ok(v.clone()),
            None => Err(Error::EmptyColumn),
        }
    }

    fn max<T: Clone + Ord>(values: &[T]) -> Result<T> {
        match values.iter().max() {
            Some(v) => Ok(v.clone()),
            None => Err(Error::EmptyColumn),
        }
    }

    fn min<T: Clone + Ord>(values: &[T]) -> Result<T> {
        match values.iter().min() {
            Some(v) => Ok(v.clone()),
            None => Err(Error::EmptyColumn),
        }
    }
}
