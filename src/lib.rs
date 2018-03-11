#![feature(box_syntax, box_patterns, io, plugin)]
#![plugin(clippy)]

extern crate csv;
extern crate decorum;
#[macro_use]
extern crate downcast_rs;
extern crate futures;
extern crate futures_cpupool;
extern crate hyper;
#[macro_use]
extern crate lazy_static;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate time;

#[macro_use]
mod timer;

pub mod aggregate;
pub mod api;
#[macro_use]
pub mod block;
pub mod dataframe;
pub mod pool;
pub mod reader;
pub mod schema;
pub mod value;

#[macro_export]
macro_rules! col {
    ( {$value:expr} ) => {
        $crate::dataframe::ColumnExpr::Constant($crate::value::Value::from($value))
    };
    ( ($source:expr) ) => {
        $crate::dataframe::ColumnExpr::Source($source.to_string())
    };
    ( $source:tt AS $alias:expr ) => {
        $crate::dataframe::ColumnExpr::Alias($alias.to_string(), Box::new(col!($source)))
    };
    ( (+ $left:tt $right:tt) ) => {
        $crate::dataframe::ColumnExpr::Operation(
            $crate::block::ArithmeticOp::Add, Box::new(col!($left)), Box::new(col!($right))
        )
    };
    ( (- $left:tt $right:tt) ) => {
        $crate::dataframe::ColumnExpr::Operation(
            $crate::block::ArithmeticOp::Subtract, Box::new(col!($left)), Box::new(col!($right))
        )
    };
    ( (* $left:tt $right:tt) ) => {
        $crate::dataframe::ColumnExpr::Operation(
            $crate::block::ArithmeticOp::Multiply, Box::new(col!($left)), Box::new(col!($right))
        )
    };
    ( (/ $left:tt $right:tt) ) => {
        $crate::dataframe::ColumnExpr::Operation(
            $crate::block::ArithmeticOp::Divide, Box::new(col!($left)), Box::new(col!($right))
        )
    };
}

#[macro_export]
macro_rules! agg {
    ( $( $c:expr, $a:expr ),* ) => {{
        let mut aggregators = std::collections::HashMap::new();
        $(
            aggregators.insert($c.to_string(), $a);
        )*
        aggregators
    }};
}

#[macro_export]
macro_rules! predicate {
    ( == $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::Equal, df::value::Value::from($v))
    }};
    ( > $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::GreaterThan, df::value::Value::from($v))
    }};
    ( >= $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::GreaterThanOrEq, df::value::Value::from($v))
    }};
    ( < $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::LessThan, df::value::Value::from($v))
    }};
    ( <= $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::LessThanOrEq, df::value::Value::from($v))
    }};
}

#[derive(Debug)]
pub enum Error {
    DataFrame(dataframe::Error),
}

impl From<dataframe::Error> for Error {
    fn from(error: dataframe::Error) -> Error {
        Error::DataFrame(error)
    }
}
