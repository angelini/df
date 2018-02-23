use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::io;
use std::path::{Path, PathBuf};

use csv;

use block::{self, Block};
use schema::Schema;
use timer;

#[derive(Debug)]
pub enum Error {
    Block(block::Error),
    Csv(csv::Error),
    Io(io::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Block(ref error) => write!(f, "{}", error),
            Error::Csv(ref error) => write!(f, "{}", error),
            Error::Io(ref error) => write!(f, "{}", error),
        }
    }
}

impl From<block::Error> for Error {
    fn from(error: block::Error) -> Error {
        Error::Block(error)
    }
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

type Result<T> = ::std::result::Result<T, Error>;

trait Reader {
    fn indices(&self) -> HashMap<String, u64>;
    fn read(&self) -> Result<HashMap<String, Box<Block>>>;
}

struct CsvReader {
    path: PathBuf,
    schema: Schema,
}

impl CsvReader {
    fn new(path: &Path, schema: &Schema) -> Self {
        CsvReader {
            path: path.to_path_buf(),
            schema: schema.clone(),
        }
    }

    fn generate_index(&self, column_name: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.path.hash(&mut hasher);
        column_name.hash(&mut hasher);
        hasher.finish()
    }
}

impl Reader for CsvReader {
    fn indices(&self) -> HashMap<String, u64> {
        self.schema
            .keys()
            .iter()
            .map(|column_name| {
                (column_name.to_string(), self.generate_index(column_name))
            })
            .collect()
    }

    fn read(&self) -> Result<HashMap<String, Box<Block>>> {
        let file = File::open(&self.path)?;
        let mut csv_reader = csv::ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b'|')
            .from_reader(file);
        let mut string_blocks = HashMap::new();
        timer::start(101, "read_csv - read from disk");
        for record in csv_reader.records() {
            let record = record?;
            for (idx, column) in self.schema.iter().enumerate() {
                let entry = string_blocks.entry(&column.name).or_insert_with(|| vec![]);
                // FIXME: pass references to the conversion fns
                entry.push(record.get(idx).unwrap().to_string())
            }
        }
        timer::stop(101);
        timer::start(102, "read_csv - cast vecs to Blocks");
        let blocks =
            string_blocks
                .into_iter()
                .map(|(name, vals)| {
                    let type_ = &self.schema.type_(name).unwrap();
                    Ok((
                        name.to_string(),
                        block::from_strings(type_, vals)?,
                    ))
                })
                .collect::<::std::result::Result<HashMap<String, Box<Block>>, block::Error>>()?;
        timer::stop(102);
        Ok(blocks)
    }
}


#[derive(Clone, Debug, Deserialize, Hash, Serialize)]
pub enum Format {
    Csv,
}

impl Format {
    pub fn indices(&self, path: &Path, schema: &Schema) -> HashMap<String, u64> {
        match *self {
            Format::Csv => CsvReader::new(path, schema).indices()
        }
    }

    pub fn read(&self, path: &Path, schema: &Schema) -> Result<HashMap<String, Box<Block>>> {
        match *self {
            Format::Csv => CsvReader::new(path, schema).read()
        }
    }
}
