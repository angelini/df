use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::path::Path;

use csv;

use dataframe::{self, DataFrame, Schema};
use pool::Pool;
use timer;
use value::{self, Values};

#[derive(Debug)]
pub enum Error {
    Csv(csv::Error),
    DataFrame(dataframe::Error),
    Io(io::Error),
    Value(value::Error),
}

impl From<csv::Error> for Error {
    fn from(error: csv::Error) -> Error {
        Error::Csv(error)
    }
}

impl From<dataframe::Error> for Error {
    fn from(error: dataframe::Error) -> Error {
        Error::DataFrame(error)
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

type Result<T> = ::std::result::Result<T, Error>;

pub fn from_csv(pool: &mut Pool, path: &Path, schema: &Schema) -> Result<DataFrame> {
    timer::start(101, "from_csv - read file");
    let file = File::open(path)?;
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b'|')
        .from_reader(file);
    let mut raw_values = HashMap::new();
    for record in csv_reader.records() {
        let record = record?;
        for (idx, column) in schema.iter().enumerate() {
            let entry = raw_values.entry(&column.name).or_insert_with(|| vec![]);
            entry.push(record.get(idx).unwrap().to_string())
        }
    }
    timer::stop(101);
    timer::start(102, "from_csv - cast vecs to Values");
    let values = raw_values
        .into_iter()
        .map(|(name, vals)| {
            let type_ = &schema.type_(name).unwrap();
            Ok((
                name.to_string(),
                Values::convert_from_strings(type_, &vals)?,
            ))
        })
        .collect::<::std::result::Result<HashMap<String, Values>, value::Error>>()?;
    timer::stop(102);
    Ok(DataFrame::new(pool, schema.clone(), values))
}
