#[macro_use]
extern crate df;
extern crate hyper;

use std::env;
use std::path::Path;
use std::sync::Arc;

use hyper::server::Http;

use df::aggregate::Aggregator;
use df::dataframe::Schema;
use df::pool::Pool;
use df::value::Type;

fn example() -> Result<(), df::Error> {
    let pool = Pool::new_ref();
    let schema = Schema::new(
        &[
            ("order_key", Type::Int),
            ("part_key", Type::Int),
            ("supplier_key", Type::Int),
            ("line_number", Type::Int),
            ("quantity", Type::Float),
            ("extended_price", Type::Float),
            ("discount", Type::Float),
            ("tax", Type::Float),
            ("return_flag", Type::String),
            ("line_status", Type::String),
            ("ship_date", Type::String),
            ("commit_date", Type::String),
            ("receipt_date", Type::String),
            ("shipping_instructions", Type::String),
            ("shipping_mode", Type::String),
            ("comment", Type::String),
        ],
    );

    let line_items = df::from_csv(&pool, &Path::new("./data/line_items.csv"), &schema)?;
    let total_key = line_items.select(&["order_key"])?.aggregate(&agg!(
        "order_key",
        Aggregator::Sum
    ))?;
    df::timer::start(201, "collect");
    println!(
        "total_key.collect(&mut pool): {:?}",
        total_key.collect(&pool)
    );
    df::timer::stop(201);
    Ok(())
}

fn server() -> Result<(), df::Error> {
    let addr = "127.0.0.1:3000".parse().unwrap();
    let pool = Pool::new_ref();
    let server = Http::new()
        .bind(&addr, move || Ok(df::api::Api::new(Arc::clone(&pool))))
        .unwrap();
    server.run().unwrap();
    Ok(())
}

fn main() {
    if let Some(command) = env::args().nth(1) {
        match command.as_str() {
            "example" => example().expect("Error in example"),
            "server" => server().expect("Error in server"),
            _ => panic!("Unknown command")
        }
    }
}
