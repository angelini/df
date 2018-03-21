use std::collections::HashMap;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::fs;
use std::hash::{Hash, Hasher};
use std::io::{self, BufRead, BufReader, Read, Seek};
use std::path::{Path, PathBuf};

use csv;
use futures::future::{self, Future};
use futures_cpupool::CpuPool;

use block::{self, Block};
use schema::Schema;
use value::{self, Type, Value};

#[derive(Debug)]
pub enum Error {
    Block(block::Error),
    Chars(io::CharsError),
    Csv(csv::Error),
    Io(io::Error),
    Value(value::Error),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Block(ref error) => write!(f, "{}", error),
            Error::Chars(ref error) => write!(f, "{}", error),
            Error::Csv(ref error) => write!(f, "{}", error),
            Error::Io(ref error) => write!(f, "{}", error),
            Error::Value(ref error) => write!(f, "{}", error),
        }
    }
}

impl From<block::Error> for Error {
    fn from(error: block::Error) -> Error {
        Error::Block(error)
    }
}

impl From<io::CharsError> for Error {
    fn from(error: io::CharsError) -> Error {
        Error::Chars(error)
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

impl From<value::Error> for Error {
    fn from(error: value::Error) -> Error {
        Error::Value(error)
    }
}

type Result<T> = ::std::result::Result<T, Error>;

trait Reader {
    fn indices(&self) -> HashMap<String, u64>;
    fn read(&self) -> Result<HashMap<String, Box<Block>>>;
}

const FILE_CHUNK_SIZE: u64 = 1_000_000;

type BlockMap = HashMap<String, Box<Block>>;

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

    fn calculate_spans(&self) -> Result<Vec<(u64, u64)>> {
        let mut spans = vec![];
        let file = fs::File::open(&self.path)?;
        let metadata = fs::metadata(&self.path)?;
        let mut buf_reader = BufReader::new(file);

        let mut idx: u64 = 0;
        loop {
            if idx + FILE_CHUNK_SIZE >= metadata.len() {
                spans.push((idx, metadata.len()));
                return Ok(spans);
            }
            buf_reader.seek(io::SeekFrom::Start(idx + FILE_CHUNK_SIZE))?;
            let mut buf = vec![];
            buf_reader.read_until(b'\n', &mut buf)?;
            spans.push((idx, idx + FILE_CHUNK_SIZE + buf.len() as u64));
            idx += FILE_CHUNK_SIZE + buf.len() as u64;
        }
    }

    fn merge_block_maps(mut accumulator: BlockMap, other: &mut BlockMap) -> Result<BlockMap> {
        let col_names = accumulator.keys().cloned().collect::<Vec<String>>();
        for col_name in col_names {
            let mut new_block = other.remove(&col_name).unwrap();
            let mut acc_block = accumulator.entry(col_name).or_insert_with(
                || unreachable!(),
            );
            acc_block.union(new_block.as_mut())?;
        }
        Ok(accumulator)
    }
}

fn read_section(
    path: &Path,
    schema: &Schema,
    start: u64,
    end: u64,
) -> Result<HashMap<String, Box<Block>>> {
    let mut file = fs::File::open(&path)?;
    let mut contents = vec![0u8; (end - start) as usize];
    file.seek(io::SeekFrom::Start(start))?;
    file.read_exact(&mut contents)?;
    let mut csv_reader = csv::ReaderBuilder::new()
        .has_headers(false)
        .delimiter(b',')
        .from_reader(contents.as_slice());

    let types = schema.iter().map(|c| &c.type_).collect::<Vec<&Type>>();
    let mut builders = block::builders(&types);
    for record in csv_reader.records() {
        let record = record?;
        for (idx, column) in schema.iter().enumerate() {
            // println!("column: {:?}", column);
            // println!("record.get(idx).unwrap(): {:?}", record.get(idx).unwrap());
            builders[idx].push(Value::parse(
                &column.type_,
                record.get(idx).unwrap(),
            )?)?;
        }
    }
    Ok(
        schema
            .iter()
            .map(|column| {
                (column.name.to_string(), builders.remove(0).build())
            })
            .collect(),
    )
}

fn read_future(
    path: PathBuf,
    schema: Schema,
    start: u64,
    end: u64,
) -> Box<Future<Item = HashMap<String, Box<Block>>, Error = Error> + Send> {
    Box::new(future::lazy(
        move || match read_section(&path, &schema, start, end) {
            Ok(map) => future::ok(map),
            Err(err) => future::err(err),
        },
    ))
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
        let id = timer_start!("CsvReader");
        let cpu_pool = CpuPool::new_num_cpus();
        let mut futures = vec![];
        for (start, end) in self.calculate_spans()? {
            futures.push(cpu_pool.spawn(read_future(
                self.path.clone(),
                self.schema.clone(),
                start,
                end,
            )));
        }
        let result = future::join_all(futures)
            .map(|mut block_maps| {
                let first = block_maps.remove(0);
                block_maps.into_iter().fold(
                    Ok(first),
                    |acc, mut block_map| {
                        acc.and_then(|acc| Self::merge_block_maps(acc, &mut block_map))
                    },
                )
            })
            .wait();
        timer_stop!(id);
        result?
    }
}

#[derive(Clone, Debug, Deserialize, Hash, Serialize)]
pub enum Format {
    Csv,
}

impl Format {
    pub fn indices(&self, path: &Path, schema: &Schema) -> HashMap<String, u64> {
        match *self {
            Format::Csv => CsvReader::new(path, schema).indices(),
        }
    }

    pub fn read(&self, path: &Path, schema: &Schema) -> Result<HashMap<String, Box<Block>>> {
        match *self {
            Format::Csv => CsvReader::new(path, schema).read(),
        }
    }
}
