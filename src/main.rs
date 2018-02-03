#![feature(box_syntax, box_patterns, plugin)]

#![plugin(clippy)]

extern crate decorum;
extern crate rand;

mod aggregate;
mod dataframe;
mod pool;
mod value;

use std::collections::{BTreeMap, HashMap};

use aggregate::Aggregator;
use dataframe::{DataFrame, Schema};
use pool::Pool;
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

fn examples(pool: &mut Pool) -> Result<(), dataframe::Error> {
    let schema = Schema::new(&["bool", "int"], &[Type::Boolean, Type::Int]);

    let df = DataFrame::new(
        pool,
        schema,
        values!(
            "bool",
            vec![false, true, false, true, false, false],
            "int",
            vec![4, 3, 2, 1, 1, 1]
        ),
    );

    let filter_df = df.filter(
        "bool",
        Predicate::new(Comparator::Equal, Value::from(true)),
    )?;
    let select_df = filter_df.select(&["int"])?;
    println!("select_df: {:?}", select_df.collect(pool)?);

    let first_df = select_df.aggregate(&agg!("int", Aggregator::First))?;
    println!("first_df: {:?}", first_df.collect(pool)?);

    let max_df = select_df.aggregate(&agg!("int", Aggregator::Max))?;
    println!("max_df: {:?}", max_df.collect(pool)?);

    let sum_df = select_df.aggregate(&agg!("int", Aggregator::Sum))?;
    println!("sum_df: {:?}", sum_df.collect(pool)?);

    let sort_df = df.sort(&["int"])?;
    println!("sort_df: {:?}", sort_df.collect(pool)?);

    let group_df = df.group_by(&["int"])?;
    println!("group_df: {:?}", group_df.collect(pool)?);

    // let agg_group_df = df.group_by(&["bool"])?;
    // println!("{:?}", agg_group_df.collect(pool)?);

    let agg_group_df = df.group_by(&["bool"])?.aggregate(&agg!("int", Aggregator::Sum))?;
    println!("agg_group_df: {:?}", agg_group_df.collect(pool)?);

    Ok(())
}

fn main() {
    let mut pool = Pool::default();
    println!("pool.size(): {:?}", pool.size());
    match examples(&mut pool) {
        Ok(_) => (),
        Err(error) => println!("Error: {}", error),
    };
    println!("pool.clean(): {:?}", pool.clean());
    println!("pool.size(): {:?}", pool.size());
    match examples(&mut pool) {
        Ok(_) => (),
        Err(error) => println!("Error: {}", error),
    };
    println!("pool.clean(): {:?}", pool.clean());
    println!("pool.size(): {:?}", pool.size());
}
