#![feature(box_syntax, box_patterns, plugin)]

#![plugin(clippy)]

extern crate csv;
extern crate decorum;
extern crate futures;
extern crate fnv;
#[macro_use]
extern crate lazy_static;
extern crate hyper;
extern crate rand;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_json;
extern crate time;

pub mod aggregate;
pub mod api;
#[macro_use]
pub mod block;
pub mod dataframe;
pub mod pool;
pub mod timer;
pub mod reader;
pub mod schema;
pub mod value;

#[macro_export]
macro_rules! agg {
    ( $( $c:expr, $a:expr ),* ) => {{
        let mut aggregators = std::collections::BTreeMap::new();
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
