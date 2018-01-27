#![feature(box_syntax, box_patterns, concat_idents, plugin)]

#![plugin(clippy)]

extern crate decorum;
extern crate rand;

mod dataframe;
mod pool;
mod value;

use dataframe::{Aggregator, DataFrame, Schema};
use pool::Pool;
use std::collections::{BTreeMap, HashMap};
use value::{Comparator, Predicate, Type, Value, Values};

macro_rules! agg {
    ( $( $c:expr, $a:expr ),* ) => {{
        let mut aggregators = BTreeMap::new();
        $(
            aggregators.insert($c.to_string(), $a);
        )*
        aggregators
    }};
}

macro_rules! values {
    ( $( $c:expr, $v:expr ),* ) => {{
        let mut values = HashMap::new();
        $(
            values.insert($c.to_string(), Values::from($v));
        )*
        values
    }};
}

fn examples() -> Result<(), dataframe::Error> {
    let mut pool = Pool::default();
    let schema = Schema::new(&["bool", "int"], &[Type::Boolean, Type::Int]);

    let df = DataFrame::new(&mut pool, schema, values!(
        "bool", vec![false, true, false, true, false, false],
        "int", vec![4, 3, 2, 1, 1, 1]
    ));

    let filter_df = df.filter(
        "bool",
        Predicate::new(Comparator::Equal, Value::from(true)),
    )?;
    let select_df = filter_df.select(&["int"])?;
    println!("{:?}", select_df.collect(&mut pool)?);

    let first_df = select_df.aggregate(&agg!("int", Aggregator::First))?;
    println!("{:?}", first_df.collect(&mut pool)?);

    let max_df = select_df.aggregate(&agg!("int", Aggregator::Max))?;
    println!("{:?}", max_df.collect(&mut pool)?);

    let sum_df = select_df.aggregate(&agg!("int", Aggregator::Sum))?;
    println!("{:?}", sum_df.collect(&mut pool)?);

    let sort_df = df.sort(&["int"])?;
    println!("{:?}", sort_df.collect(&mut pool)?);

    let group_df = df.group_by(&["int"])?;
    println!("{:?}", group_df.collect(&mut pool)?);

    Ok(())
}

fn main() {
    match examples() {
        Ok(_) => (),
        Err(error) => println!("Error: {}", error),
    }
}
