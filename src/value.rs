use std::f64;
use std::fmt;
use std::num;
use std::str;

use decorum::R64;

#[derive(Debug)]
pub enum Error {
    Parse(Type, String),
}

impl From<str::ParseBoolError> for Error {
    fn from(error: str::ParseBoolError) -> Error {
        Error::Parse(Type::Bool, format!("{:?}", error))
    }
}

impl From<num::ParseIntError> for Error {
    fn from(error: num::ParseIntError) -> Error {
        Error::Parse(Type::Int, format!("{:?}", error))
    }
}

impl From<num::ParseFloatError> for Error {
    fn from(error: num::ParseFloatError) -> Error {
        Error::Parse(Type::Float, format!("{:?}", error))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Parse(ref type_, ref message) => {
                write!(f, "Error parsing value of type {:?}: {}", type_, message)
            }
        }
    }
}

type Result<T> = ::std::result::Result<T, Error>;

pub trait Nullable {
    fn is_null(&self) -> bool;
}

impl Nullable for bool {
    fn is_null(&self) -> bool {
        false
    }
}

impl Nullable for i64 {
    fn is_null(&self) -> bool {
        false
    }
}

impl Nullable for f64 {
    fn is_null(&self) -> bool {
        self.is_nan()
    }
}

impl Nullable for R64 {
    fn is_null(&self) -> bool {
        false
    }
}

impl Nullable for String {
    fn is_null(&self) -> bool {
        false
    }
}

impl<'a> Nullable for &'a str {
    fn is_null(&self) -> bool {
        false
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Serialize)]
pub enum Type {
    Bool,
    Int,
    Float,
    String,
    List(Box<Type>),
}

#[derive(Clone, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
pub enum Value {
    Bool(bool),
    Int(i64),
    Float(R64),
    String(String),
    BoolList(Vec<bool>),
    IntList(Vec<i64>),
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
    pub fn parse(type_: &Type, value: &str) -> Result<Value> {
        match *type_ {
            Type::Bool => Ok(Value::from(value.parse::<bool>()?)),
            Type::Int => Ok(Value::from(value.parse::<i64>()?)),
            Type::Float => Ok(Value::from(value.parse::<f64>()?)),
            Type::String => Ok(Value::from(value.to_string())),
            _ => panic!(),
        }
    }

    pub fn type_(&self) -> Type {
        value_type!(
            self,
            Bool,
            BoolList,
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
        Value::Bool(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Value::Int(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Value::Float(R64::from_inner(value))
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

impl<'a> From<&'a str> for Value {
    fn from(value: &str) -> Self {
        Value::String(value.to_string())
    }
}

impl From<Vec<bool>> for Value {
    fn from(value: Vec<bool>) -> Self {
        Value::BoolList(value)
    }
}

impl From<Vec<i64>> for Value {
    fn from(value: Vec<i64>) -> Self {
        Value::IntList(value)
    }
}

impl From<Vec<f64>> for Value {
    fn from(value: Vec<f64>) -> Self {
        Value::FloatList(value.into_iter().map(R64::from_inner).collect())
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

impl<'a> From<Vec<&'a str>> for Value {
    fn from(value: Vec<&str>) -> Self {
        Value::StringList(value.into_iter().map(|s| s.to_string()).collect())
    }
}

#[derive(Clone, Debug, Deserialize, Hash, Serialize)]
pub enum Comparator {
    Equal,
    GreaterThan,
    GreaterThanOrEq,
    LessThan,
    LessThanOrEq,
}

impl Comparator {
    pub fn pass<T: PartialEq + PartialOrd>(&self, left: &T, right: &T) -> bool {
        match *self {
            Comparator::Equal => left == right,
            Comparator::GreaterThan => left > right,
            Comparator::GreaterThanOrEq => left >= right,
            Comparator::LessThan => left < right,
            Comparator::LessThanOrEq => left <= right,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Hash, Serialize)]
pub struct Predicate {
    pub comparator: Comparator,
    pub value: Value,
}

impl Predicate {
    pub fn new(comparator: Comparator, value: Value) -> Predicate {
        Predicate { comparator, value }
    }
}
