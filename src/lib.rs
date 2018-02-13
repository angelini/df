#![feature(box_syntax, box_patterns, plugin)]

#![plugin(clippy)]

extern crate csv;
extern crate decorum;
extern crate futures;
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
pub mod dataframe;
pub mod pool;
pub mod timer;
pub mod serialize;
pub mod value;

pub use serialize::from_csv;

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

#[macro_export]
macro_rules! from_vecs {
    ( $p:expr, $( ($n:expr, $t:path, $v:expr) ),* ) => {{
        let schema = df::dataframe::Schema::new(
            &[ $( ($n, $t) ),* ]
        );
        let mut values = std::collections::HashMap::new();
        $(
            values.insert($n.to_string(), df::value::Values::from($v));
        )*
        df::dataframe::DataFrame::new($p, schema, values)
    }};
    ( $p:expr, $( ($n:expr, $t:path, $v:expr,) ),* ) => {{
        from_vecs!($p, $(($n, $t, $v)),*)
    }};
}

#[derive(Debug)]
pub enum Error {
    DataFrame(dataframe::Error),
    Serialize(serialize::Error),
}

impl From<dataframe::Error> for Error {
    fn from(error: dataframe::Error) -> Error {
        Error::DataFrame(error)
    }
}

impl From<serialize::Error> for Error {
    fn from(error: serialize::Error) -> Error {
        Error::Serialize(error)
    }
}
