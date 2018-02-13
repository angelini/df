use std::collections::HashMap;

use futures::Stream;
use futures::future::{self, Future};
use hyper::{self, Method, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Request, Response, Service};
use serde_json;

use dataframe::{self, DataFrame, Operation};
use serialize::from_csv;
use value::Values;

#[derive(Debug)]
enum Error {
    DataFrame(dataframe::Error),
    MalformedJSON,
    MissingDataFrame,
}

impl Error {
    fn status(&self) -> StatusCode {
        StatusCode::BadRequest
    }
}

impl From<dataframe::Error> for Error {
    fn from(error: dataframe::Error) -> Error {
        Error::DataFrame(error)
    }
}

type Result<T> = ::std::result::Result<T, Error>;

#[derive(Deserialize, Serialize)]
enum RequestFunction {
    Op(Operation),
    Read(String, String),
}

#[derive(Deserialize, Serialize)]
struct RequestBody {
    dataframe: Option<DataFrame>,
    function: RequestFunction,
}

#[derive(Deserialize, Serialize)]
struct ResponseBody {
    dataframe: DataFrame,
    values: HashMap<String, Values>,
}

impl ResponseBody {
    fn from_dataframe(dataframe: DataFrame) -> ResponseBody {
        ResponseBody {
            dataframe,
            values: HashMap::new(),
        }
    }
}

fn as_strs(strings: &[String]) -> Vec<&str> {
    strings.iter().map(|s| s.as_str()).collect()
}

fn execute_op(df: &DataFrame, operation: &Operation) -> Result<ResponseBody> {
    Ok(match *operation {
        Operation::Select(ref column_names) => {
            ResponseBody::from_dataframe(df.select(&as_strs(column_names))?)
        }
        Operation::Filter(ref column_name, ref predicate) => {
            ResponseBody::from_dataframe(df.filter(column_name, predicate)?)
        }
        Operation::OrderBy(ref column_names) => {
            ResponseBody::from_dataframe(df.order_by(&as_strs(column_names))?)
        }
        Operation::GroupBy(ref column_names) => {
            ResponseBody::from_dataframe(df.group_by(&as_strs(column_names))?)
        }
        Operation::Aggregation(ref aggregators) => {
            ResponseBody::from_dataframe(df.aggregate(aggregators)?)
        }
    })
}

fn read_df(scheme: &str, path: &str) -> Result<ResponseBody> {
    // match (schema, path) {
    //     ("csv", path) => from_csv
    // }
    // TODO: Thread pool into this function
    unimplemented!()
}

fn handle_request(req_body: RequestBody) -> Result<ResponseBody> {
    match req_body.function {
        RequestFunction::Op(operation) => {
            let df = req_body.dataframe.ok_or(Error::MissingDataFrame)?;
            execute_op(&df, &operation)
        }
        RequestFunction::Read(scheme, path) => read_df(&scheme, &path),
    }
}

fn serialize_response(response: Result<ResponseBody>) -> Response {
    match response {
        Ok(body) => {
            let bytes = serde_json::to_string(&body).unwrap();
            Response::new()
                .with_status(StatusCode::Created)
                .with_header(ContentLength(bytes.len() as u64))
                .with_body(bytes)
        }
        Err(error) => {
            let message = format!("{:?}", error);
            Response::new()
                .with_status(error.status())
                .with_header(ContentLength(message.len() as u64))
                .with_body(message)
        }
    }
}

pub struct Api;

impl Service for Api {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;

    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let mut response = Response::new();

        match (req.method(), req.path()) {
            (&Method::Post, "/call") => {
                return Box::new(req.body().concat2().map(|b| {
                    match serde_json::from_slice::<RequestBody>(b.as_ref()) {
                        Ok(req_body) => serialize_response(handle_request(req_body)),
                        Err(_) => serialize_response(Err(Error::MalformedJSON)),
                    }
                }));
            }
            _ => {
                response.set_status(StatusCode::NotFound);
            }
        };

        Box::new(future::ok(response))
    }
}
