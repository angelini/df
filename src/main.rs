#[macro_use]
extern crate df;

use std::path::Path;

use df::aggregate::Aggregator;
use df::dataframe::Schema;
use df::pool::Pool;
use df::value::Type;

fn run() -> Result<(), df::Error> {
    let mut pool = Pool::default();
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
    let line_items = df::from_csv(&mut pool, &Path::new("./data/line_items_full.csv"), &schema)?
        .select(&["order_key"])?
        .aggregate(&agg!("order_key", Aggregator::Sum))?;
    df::timer::start(201, "collect");
    println!(
        "line_items.collect(&mut pool): {:?}",
        line_items.collect(&mut pool)
    );
    df::timer::stop(201);
    Ok(())
}

fn main() {
    run().expect("Error in run")
}
