use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use futures::Stream;
use futures::future::{self, Future};
use hyper::{self, Method, StatusCode};
use hyper::header::ContentLength;
use hyper::server::{Request, Response, Service};
use serde_json;

use dataframe::{self, DataFrame, Operation, Schema};
use pool::PoolRef;
use serialize::{self, from_csv};
use timer;
use value::Values;

#[derive(Debug)]
enum Error {
    DataFrame(dataframe::Error),
    MalformedJSON,
    MissingDataFrame,
    Serialize(serialize::Error),
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

impl From<serialize::Error> for Error {
    fn from(error: serialize::Error) -> Error {
        Error::Serialize(error)
    }
}

type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, Deserialize, Serialize)]
enum Action {
    Collect,
    Count,
    Take(u64),
}

#[derive(Debug, Deserialize, Serialize)]
enum RequestFunction {
    Action(Action),
    Op(Operation),
    Read(String, String, Schema),
}

#[derive(Debug, Deserialize, Serialize)]
struct RequestBody {
    dataframe: Option<DataFrame>,
    function: RequestFunction,
}

#[derive(Debug, Deserialize, Serialize)]
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

fn execute_action(pool: &PoolRef, df: &DataFrame, action: &Action) -> Result<ResponseBody> {
    timer::start(301, &format!("execute action - {:?}", action));
    let values = match *action {
        Action::Collect => df.as_values(pool)?,
        _ => unimplemented!(),
    };
    timer::stop(301);
    Ok(ResponseBody {
        dataframe: df.clone(),
        values,
    })
}

fn execute_op(df: &DataFrame, operation: &Operation) -> Result<ResponseBody> {
    Ok(ResponseBody::from_dataframe(df.call(operation)?))
}

fn read_df(pool: &PoolRef, format: &str, path: &str, schema: &Schema) -> Result<ResponseBody> {
    let df = match (format, path) {
        ("csv", path) => from_csv(pool, Path::new(path), schema),
        _ => unimplemented!(),
    };
    Ok(ResponseBody::from_dataframe(df?))
}

fn handle_request(pool: &PoolRef, req_body: RequestBody) -> Result<ResponseBody> {
    match req_body.function {
        RequestFunction::Action(action) => {
            let df = req_body.dataframe.ok_or(Error::MissingDataFrame)?;
            execute_action(pool, &df, &action)
        }
        RequestFunction::Op(operation) => {
            let df = req_body.dataframe.ok_or(Error::MissingDataFrame)?;
            execute_op(&df, &operation)
        }
        RequestFunction::Read(scheme, path, schema) => read_df(pool, &scheme, &path, &schema),
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

pub struct Api {
    pool: PoolRef,
}

impl Api {
    pub fn new(pool: PoolRef) -> Self {
        Api { pool }
    }
}

impl Service for Api {
    type Request = Request;
    type Response = Response;
    type Error = hyper::Error;

    type Future = Box<Future<Item = Self::Response, Error = Self::Error>>;

    fn call(&self, req: Request) -> Self::Future {
        let mut response = Response::new();
        match (req.method(), req.path()) {
            (&Method::Post, "/call") => {
                let pool = Arc::clone(&self.pool);
                let res_future = req.body().concat2().map(move |b| {
                    match serde_json::from_slice::<RequestBody>(b.as_ref()) {
                        Ok(req_body) => serialize_response(handle_request(&pool, req_body)),
                        Err(_) => serialize_response(Err(Error::MalformedJSON)),
                    }
                });
                return box res_future;
            }
            _ => {
                response.set_status(StatusCode::NotFound);
            }
        };
        box future::ok(response)
    }
}
