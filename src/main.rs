#![feature(plugin)]

#![plugin(clippy)]

mod dataframe;
mod pool;
mod value;

extern crate rand;

use dataframe::{Aggregator, DataFrame, Schema};
use pool::Pool;
use std::collections::{BTreeMap, HashMap};
use value::{Comparator, Predicate, Type, Value, Values};

fn main() {
    let mut pool = Pool::default();
    let schema = Schema::new(&["bool", "int"], &[Type::Boolean, Type::Int]);

    let mut values = HashMap::new();
    values.insert("bool".to_string(), Values::from(vec![true, false, true]));
    values.insert("int".to_string(), Values::from(vec![1, 2, 3]));

    let df = DataFrame::new(&mut pool, schema, values);

    let filter_df = df.filter("bool", Predicate::new(Comparator::Equal, Value::from(true)));
    let select_df = filter_df.select(&["int"]);
    println!("{:?}", select_df.collect(&mut pool));

    let mut aggregators = BTreeMap::new();
    aggregators.insert("int".to_string(), Aggregator::First);
    let first_df = select_df.aggregate(aggregators);
    println!("{:?}", first_df.collect(&mut pool));

    let mut aggregators = BTreeMap::new();
    aggregators.insert("int".to_string(), Aggregator::Max);
    let max_df = select_df.aggregate(aggregators);
    println!("{:?}", max_df.collect(&mut pool));

    let mut aggregators = BTreeMap::new();
    aggregators.insert("int".to_string(), Aggregator::Sum);
    let sum_df = select_df.aggregate(aggregators);
    println!("{:?}", sum_df.collect(&mut pool));
}
