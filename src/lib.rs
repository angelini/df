#![feature(box_syntax, box_patterns, plugin)]

#![plugin(clippy)]

extern crate csv;
extern crate decorum;
extern crate rand;

pub mod aggregate;
pub mod dataframe;
pub mod pool;
pub mod value;

use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::Path;
use std::result;

use dataframe::{DataFrame, Schema};
use pool::Pool;
use value::Values;

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
            &[ $( $n, )* ],
            &[ $( $t, )* ],
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
    Csv(csv::Error),
    Io(io::Error),
    Value(value::Error),
}

impl From<csv::Error> for Error {
    fn from(error: csv::Error) -> Error {
        Error::Csv(error)
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Error {
        Error::Io(error)
    }
}

impl From<value::Error> for Error {
    fn from(error: value::Error) -> Error {
        Error::Value(error)
    }
}

type Result<T> = result::Result<T, Error>;

pub fn from_csv(pool: &mut Pool, path: &Path, schema: &Schema) -> Result<DataFrame> {
    let file = File::open(path)?;
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'|')
        .from_reader(file);
    let mut raw_values = HashMap::new();
    for record in csv_reader.records() {
        let record = record?;
        for (idx, (name, _)) in schema.columns.iter().enumerate() {
            let entry = raw_values.entry(name).or_insert_with(|| vec![]);
            entry.push(record.get(idx).unwrap().to_string())
        }
    }
    let values = raw_values.into_iter()
        .map(|(name, vals)| {
            let type_ = &schema.columns[name].type_;
            Ok((name.to_string(), Values::convert_from_strings(type_, &vals)?))
        })
        .collect::<result::Result<HashMap<String, Values>, value::Error>>()?;
    Ok(DataFrame::new(pool, schema.clone(), values))
}
