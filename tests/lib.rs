#![feature(custom_attribute)]

extern crate decorum;
extern crate tempdir;

#[macro_use]
extern crate df;

use decorum::R64;
use tempdir::TempDir;

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use df::aggregate::Aggregator;
use df::dataframe::{DataFrame, Result, Schema};
use df::pool::Pool;
use df::value::Type;

macro_rules! assert_df_eq {
    ( $p:expr, $d:expr, $( ( $($v:expr),* ) ),* ) => {
        let rows = $d.collect($p).expect("Cannot collect df");
        let expected = vec![$(
            df::dataframe::Row::new(vec![$( df::value::Value::from($v), )*]),
        )*];
        assert_eq!(rows, expected);
    };
}

fn check(result: Result<DataFrame>) -> DataFrame {
    result.expect("Cannot build dataframe")
}

fn write_csv(dir: &TempDir, name: &str, rows: &[&str]) -> PathBuf {
    let path = dir.path().join(name);
    let mut file = File::create(path.clone()).unwrap();
    for row in rows {
        writeln!(file, "{}", row).unwrap()
    }
    path
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

#[test]
fn test_select() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("bool", Type::Boolean, vec![true, false, true]),
        ("int", Type::Int, vec![1, 2, 3])
    );
    let output = df.select(&["int"]);
    assert_df_eq!(&mut pool, check(output), (1), (2), (3));
}

#[test]
fn test_filter_eq() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("bool", Type::Boolean, vec![true, false, true]),
        ("int", Type::Int, vec![1, 2, 3])
    );
    let output = df.filter("int", predicate!(== 2));
    assert_df_eq!(&mut pool, check(output), (false, 2));
}

#[test]
fn test_filter_select() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("bool", Type::Boolean, vec![true, false, true]),
        ("int", Type::Int, vec![1, 2, 3])
    );
    let output = || df.filter("int", predicate!(== 2))?.select(&["bool"]);
    assert_df_eq!(&mut pool, check(output()), (false));
}

#[test]
#[allow(unused_attributes)]
#[rustfmt_skip]
fn test_order_by() {
    let mut pool = Pool::default();
    let df = from_vecs!(&mut pool,
                        ("1_int", Type::Int, vec![4, 1, 6]),
                        ("2_int", Type::Int, vec![1, 2, 3]));
    let output = df.order_by(&["1_int"]);
    assert_df_eq!(&mut pool, check(output), (1, 2), (4, 1), (6, 3));
}

#[test]
fn test_order_by_multiple_columns() {
    let mut pool = Pool::default();
    let df =
        from_vecs!(&mut pool,
                   ("1_int", Type::Int, vec![4, 1, 6, 4, 1]),
                   ("2_int", Type::Int, vec![3, 1, 1, 1, 2]),
                   ("3_int", Type::Int, vec![1, 2, 3, 4, 5]));
    let output = df.order_by(&["1_int", "2_int"]);
    assert_df_eq!(
        &mut pool,
        check(output),
        (1, 1, 2),
        (1, 2, 5),
        (4, 1, 4),
        (4, 3, 1),
        (6, 1, 3)
    );
}

#[test]
fn test_group_by_only_keys() {
    let mut pool = Pool::default();
    let df = from_vecs!(&mut pool, ("int", Type::Int, vec![2, 1, 2, 3]));
    let output = df.group_by(&["int"]);
    assert_df_eq!(&mut pool, check(output), (1), (2), (3));
}

#[test]
#[allow(unused_attributes)]
#[rustfmt_skip]
fn test_group_by() {
    let mut pool = Pool::default();
    let df = from_vecs!(&mut pool,
                        ("1_int", Type::Int, vec![3, 2, 1, 2]),
                        ("2_bool", Type::Boolean, vec![true, false, true, true]));
    let output = df.group_by(&["1_int"]);
    assert_df_eq!(
        &mut pool,
        check(output),
        (1, vec![true]),
        (2, vec![false, true]),
        (3, vec![true])
    );
}

#[test]
fn test_group_by_multiple_columns() {
    let mut pool = Pool::default();
    let df =
        from_vecs!(&mut pool,
                        ("1_int", Type::Int, vec![3, 2, 1, 2, 2]),
                        ("2_int", Type::Int, vec![4, 3, 2, 1, 3]),
                        ("3_bool", Type::Boolean, vec![true, false, true, false, true]));
    let output = df.group_by(&["1_int", "2_int"]);
    assert_df_eq!(
        &mut pool,
        check(output),
        (1, 2, vec![true]),
        (2, 1, vec![false]),
        (2, 3, vec![false, true]),
        (3, 4, vec![true])
    );
}

#[test]
fn test_group_agg() {
    let mut pool = Pool::default();
    let df = from_vecs!(
        &mut pool,
        ("bool", Type::Boolean, vec![true, false, true]),
        ("int", Type::Int, vec![1, 2, 3])
    );
    let output = || {
        df.group_by(&["bool"])?.aggregate(
            &agg!("int", Aggregator::Sum),
        )
    };
    assert_df_eq!(&mut pool, check(output()), (false, 2), (true, 4));
}

#[test]
fn test_agg_sum() {
    let mut pool = Pool::default();
    let df = from_vecs!(&mut pool, ("int", Type::Int, vec![1, 2, 3]));
    let output = df.aggregate(&agg!("int", Aggregator::Sum));
    assert_df_eq!(&mut pool, check(output), (6));
}

#[test]
fn test_agg_first() {
    let mut pool = Pool::default();
    let df = from_vecs!(&mut pool, ("int", Type::Int, vec![1, 2, 3]));
    let output = df.aggregate(&agg!("int", Aggregator::First));
    assert_df_eq!(&mut pool, check(output), (1));
}

#[test]
fn test_agg_max() {
    let mut pool = Pool::default();
    let df = from_vecs!(&mut pool, ("int", Type::Int, vec![1, 2, 3, 2]));
    let output = df.aggregate(&agg!("int", Aggregator::Max));
    assert_df_eq!(&mut pool, check(output), (3));
}

#[test]
fn test_agg_min() {
    let mut pool = Pool::default();
    let df = from_vecs!(&mut pool, ("int", Type::Int, vec![2, 1, 2, 3]));
    let output = df.aggregate(&agg!("int", Aggregator::Min));
    assert_df_eq!(&mut pool, check(output), (1));
}

#[test]
fn test_agg_multiple_columns() {
    let mut pool = Pool::default();
    let df =
        from_vecs!(&mut pool,
                   ("1_int", Type::Int, vec![4, 1, 6, 4, 1]),
                   ("2_int", Type::Int, vec![3, 1, 1, 1, 2]),
                   ("3_int", Type::Int, vec![1, 2, 3, 4, 5]));
    let output = df.aggregate(&agg!(
        "1_int",
        Aggregator::Sum,
        "2_int",
        Aggregator::Min,
        "3_int",
        Aggregator::Max
    ));
    assert_df_eq!(&mut pool, check(output), (16, 1, 5));
}

#[test]
fn test_pool_clean() {
    let mut pool = Pool::default();
    assert_eq!(pool.size(), 0);
    let df = from_vecs!(&mut pool, ("int", Type::Int, vec![2, 1, 2, 3]));
    let df2 = from_vecs!(&mut pool, ("int", Type::Int, vec![2, 1, 2, 3]));
    check(df.order_by(&["int"])).collect(&mut pool).unwrap();
    check(df2.order_by(&["int"])).collect(&mut pool).unwrap();
    assert_eq!(pool.size(), 4);
    assert_eq!(pool.clean(), 4);
    assert_eq!(pool.size(), 0);
}

#[test]
fn test_from_csv() {
    let dir = TempDir::new("test_from_csv").unwrap();
    let path = write_csv(&dir, "example.csv", &[
        "true|1|1.0|hello world",
        "false|4|1.2|fOObAr"
    ]);
    let mut pool = Pool::default();
    let schema = Schema::new(
        &["1_bool", "2_int", "3_float", "4_string"],
        &[Type::Boolean, Type::Int, Type::Float, Type::String],
    );
    let df = df::from_csv(&mut pool, path.as_path(), &schema);
    assert_df_eq!(&mut pool, df.expect("Cannot build df from csv"),
                  (true, 1, R64::from_inner(1.0), "hello world".to_string()),
                  (false, 4, R64::from_inner(1.2), "fOObAr".to_string()));
}
