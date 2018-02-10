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
            "01_order_key",
            "02_part_key",
            "03_supplier_key",
            "04_line_number",
            "05_quantity",
            "06_extended_price",
            "07_discount",
            "08_tax",
            "09_return_flag",
            "10_line_status",
            "11_ship_date",
            "12_commit_date",
            "13_receipt_date",
            "14_shipping_instructions",
            "15_shipping_mode",
            "16_comment",
        ],
        &[
            Type::Int,
            Type::Int,
            Type::Int,
            Type::Int,
            Type::Float,
            Type::Float,
            Type::Float,
            Type::Float,
            Type::String,
            Type::String,
            Type::String,
            Type::String,
            Type::String,
            Type::String,
            Type::String,
            Type::String,
        ],
    );
    let line_items = df::from_csv(&mut pool, &Path::new("./line_items.csv"), &schema)?
        .select(&["01_order_key"])?
    .aggregate(&agg!("01_order_key", Aggregator::Sum))?;
    df::timer::start(2, "collect");
    println!(
        "line_items.collect(&mut pool): {:?}",
        line_items.collect(&mut pool)
    );
    df::timer::stop(2);
    Ok(())
}

fn main() {
    run().expect("Error in run")
}
