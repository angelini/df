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

fn examples() -> Result<(), dataframe::Error> {
    let mut pool = Pool::default();
    let schema = Schema::new(&["bool", "int"], &[Type::Boolean, Type::Int]);

    let mut values = HashMap::new();
    values.insert(
        "bool".to_string(),
        Values::from(vec![false, true, false, true, false, false]),
    );
    values.insert("int".to_string(), Values::from(vec![4, 3, 2, 1, 1, 1]));

    let df = DataFrame::new(&mut pool, schema, values);

    let filter_df = df.filter(
        "bool",
        Predicate::new(Comparator::Equal, Value::from(true)),
    )?;
    let select_df = filter_df.select(&["int"])?;
    println!("{:?}", select_df.collect(&mut pool)?);

    let mut aggregators = BTreeMap::new();
    aggregators.insert("int".to_string(), Aggregator::First);
    let first_df = select_df.aggregate(&aggregators)?;
    println!("{:?}", first_df.collect(&mut pool)?);

    let mut aggregators = BTreeMap::new();
    aggregators.insert("int".to_string(), Aggregator::Max);
    let max_df = select_df.aggregate(&aggregators)?;
    println!("{:?}", max_df.collect(&mut pool)?);

    let mut aggregators = BTreeMap::new();
    aggregators.insert("int".to_string(), Aggregator::Sum);
    let sum_df = select_df.aggregate(&aggregators)?;
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
