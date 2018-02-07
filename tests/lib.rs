#[macro_use]
extern crate df;

use df::aggregate::Aggregator;
use df::dataframe::{DataFrame, Result};
use df::pool::Pool;
use df::value::{Comparator, Predicate, Value, Type};

macro_rules! assert_df_eq {
    ( $p:expr, $d:expr, $( ( $($v:expr),* ) ),* ) => {
        let rows = $d.collect($p).expect("Cannot collect df");
        let expected = vec![$(
            df::dataframe::Row::new(vec![$( df::value::Value::from($v), )*]),
        )*];
        assert_eq!(rows, expected);
    }
}

fn check(result: Result<DataFrame>) -> DataFrame {
    result.expect("Cannot build dataframe")
}

#[test]
fn test_no_transforms() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("bool", Type::Boolean, vec![true, false, true]),
        ("int", Type::Int, vec![1, 2, 3])
    );
    assert_df_eq!(&mut pool, df, (true, 1), (false, 2), (true, 3));
}

fn select_query(df: DataFrame) -> Result<DataFrame> {
    df.select(&["int"])
}

#[test]
fn test_select() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("bool", Type::Boolean, vec![true, false, true]),
        ("int", Type::Int, vec![1, 2, 3])
    );
    assert_df_eq!(&mut pool, check(select_query(df)), (1), (2), (3));
}

fn filter_eq_query(df: DataFrame) -> Result<DataFrame> {
    df.filter("int", Predicate::new(Comparator::Equal, Value::from(2)))
}

#[test]
fn test_filter_eq() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("bool", Type::Boolean, vec![true, false, true]),
        ("int", Type::Int, vec![1, 2, 3])
    );
    assert_df_eq!(&mut pool, check(filter_eq_query(df)), (false, 2));
}

fn filter_select_query(df: DataFrame) -> Result<DataFrame> {
    df.filter("int", Predicate::new(Comparator::Equal, Value::from(2)))?
        .select(&["bool"])

}

#[test]
fn test_filter_select() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("bool", Type::Boolean, vec![true, false, true]),
        ("int", Type::Int, vec![1, 2, 3])
    );
    assert_df_eq!(&mut pool, check(filter_select_query(df)), (false));
}

fn sort_query(df: DataFrame) -> Result<DataFrame> {
    df.sort(&["int_1"])
}

#[test]
fn test_sort() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("int_1", Type::Int, vec![4, 1, 6]),
        ("int_2", Type::Int, vec![1, 2, 3])
    );
    assert_df_eq!(&mut pool, check(sort_query(df)), (1, 2), (4, 1), (6, 3));
}

fn agg_sum_query(df: DataFrame) -> Result<DataFrame> {
    df.aggregate(&agg!("int", Aggregator::Sum))
}

#[test]
fn test_agg_sum() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("int", Type::Int, vec![1, 2, 3])
    );
    assert_df_eq!(&mut pool, check(agg_sum_query(df)), (6));
}

fn agg_first_query(df: DataFrame) -> Result<DataFrame> {
    df.aggregate(&agg!("int", Aggregator::First))
}

#[test]
fn test_agg_first() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("int", Type::Int, vec![1, 2, 3])
    );
    assert_df_eq!(&mut pool, check(agg_first_query(df)), (1));
}

fn agg_max_query(df: DataFrame) -> Result<DataFrame> {
    df.aggregate(&agg!("int", Aggregator::Max))
}

#[test]
fn test_agg_max() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("int", Type::Int, vec![1, 2, 3, 2])
    );
    assert_df_eq!(&mut pool, check(agg_max_query(df)), (3));
}

fn agg_min_query(df: DataFrame) -> Result<DataFrame> {
    df.aggregate(&agg!("int", Aggregator::Min))
}

#[test]
fn test_agg_min() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("int", Type::Int, vec![2, 1, 2, 3])
    );
    assert_df_eq!(&mut pool, check(agg_min_query(df)), (1));
}
