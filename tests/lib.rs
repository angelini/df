#![feature(custom_attribute)]

extern crate decorum;
extern crate tempdir;
extern crate serde_json;

#[macro_use]
extern crate df;

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;

use tempdir::TempDir;

use df::aggregate::Aggregator;
use df::dataframe::{DataFrame, Result};
use df::pool::Pool;
use df::reader::Format;
use df::schema::Schema;
use df::value::Type;

macro_rules! assert_df_eq {
    ( $p:expr, $d:expr, $( ( $($v:expr),* ) ),* ) => {
        let rows = $d.collect($p).expect("Cannot collect df");
        let expected = vec![$(
            df::dataframe::Row::new(vec![$( df::value::Value::from($v), )*]),
        )*];
        assert_eq!(rows, expected);
    };
    ( $p:expr, $d:expr, $( [ $($v:expr),* ] ),* ) => {
        assert_df_eq!($p, $d, $(($($v),*)),*)
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
    file.flush().expect("Cannot flush csv");
    path
}

#[test]
fn test_no_transforms() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["bool", Type::Bool, vec![true, false, true]],
        ["int", Type::Int, vec![1, 2, 3]]
    );
    assert_df_eq!(&pool, df, (true, 1), (false, 2), (true, 3));
}

#[test]
fn test_select() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["bool", Type::Bool, vec![true, false, true]],
        ["int", Type::Int, vec![1, 2, 3]]
    );
    let output = df.select(&[col!(("int"))]);
    assert_df_eq!(&pool, check(output), (1), (2), (3));
}

#[test]
fn test_select_combine_with_constants() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["bool", Type::Bool, vec![true, false, true]],
        ["int", Type::Int, vec![1, 2, 3]]
    );
    let output = df.select(&[col!((+ ("int") {1}) AS "add_1")]);
    assert_df_eq!(&pool, check(output), (2), (3), (4));

    let output = df.select(&[col!((- ("int") {1}) AS "sub_1")]);
    assert_df_eq!(&pool, check(output), (0), (1), (2));

    let output = df.select(&[col!((* ("int") {2}) AS "mul_2")]);
    assert_df_eq!(&pool, check(output), (2), (4), (6));

    let output = df.select(&[col!((/ ("int") {2}) AS "div_2")]);
    assert_df_eq!(&pool, check(output), (0.5), (1.0), (1.5));
}

#[test]
fn test_filter_int_eq() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["bool", Type::Bool, vec![true, false, true]],
        ["int", Type::Int, vec![1, 2, 3]]
    );
    let output = df.filter("int", &predicate!(== 2));
    assert_df_eq!(&pool, check(output), (false, 2));
}

#[test]
fn test_filter_string_eq() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["bool", Type::Bool, vec![true, false, true]],
        ["string", Type::String, vec!["foo", "bar", "baz"]]
    );
    let output = df.filter("string", &predicate!(== "baz"));
    assert_df_eq!(&pool, check(output), (true, "baz"));
}

#[test]
fn test_filter_select() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["bool", Type::Bool, vec![true, false, true]],
        ["int", Type::Int, vec![1, 2, 3]]
    );
    let output = || {
        df.filter("int", &predicate!(== 2))?.select(
            &[col!(("bool"))],
        )
    };
    assert_df_eq!(&pool, check(output()), (false));
}

#[test]
fn test_order_by() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["1_int", Type::Int, vec![4, 1, 6]],
        ["2_int", Type::Int, vec![1, 2, 3]]
    );
    let output = df.order_by(&["1_int"]);
    assert_df_eq!(&pool, check(output), (1, 2), (4, 1), (6, 3));
}

#[test]
fn test_order_by_string() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["string", Type::String, vec!["foo", "bar", "baz"]],
        ["int", Type::Int, vec![1, 2, 3]]
    );
    let output = df.order_by(&["string"]);
    assert_df_eq!(&pool, check(output), ("bar", 2), ("baz", 3), ("foo", 1));
}

#[test]
fn test_order_by_multiple_columns() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["1_int", Type::Int, vec![4, 1, 6, 4, 1]],
        ["2_int", Type::Int, vec![3, 1, 1, 1, 2]],
        ["3_int", Type::Int, vec![1, 2, 3, 4, 5]]
    );
    let output = df.order_by(&["1_int", "2_int"]);
    assert_df_eq!(
        &pool,
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
    let pool = Pool::new_ref();
    let df = from_vecs!(&pool, ["int", Type::Int, vec![2, 1, 2, 3]]);
    let output = df.group_by(&["int"]);
    assert_df_eq!(&pool, check(output), (1), (2), (3));
}

#[test]
fn test_group_by() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["int", Type::Int, vec![3, 2, 1, 2]],
        ["bool", Type::Bool, vec![true, false, true, true]]
    );
    let output = df.group_by(&["int"]);
    assert_df_eq!(
        &pool,
        check(output),
        (1, vec![true]),
        (2, vec![false, true]),
        (3, vec![true])
    );
}

#[test]
fn test_group_by_string_key() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["string", Type::String, vec!["foo", "bar", "baz", "foo"]],
        ["int", Type::Int, vec![3, 2, 1, 2]]
    );
    let output = df.group_by(&["string"]);
    assert_df_eq!(
        &pool,
        check(output),
        ["bar", vec![2]],
        ["baz", vec![1]],
        ["foo", vec![3, 2]]
    );
}

#[test]
fn test_group_by_string_list() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["int", Type::Int, vec![3, 2, 1, 2]],
        ["string", Type::String, vec!["foo", "bar", "baz", "qux"]]
    );
    let output = df.group_by(&["int"]);
    assert_df_eq!(
        &pool,
        check(output),
        (1, vec!["baz"]),
        (2, vec!["bar", "qux"]),
        (3, vec!["foo"])
    );
}

#[test]
fn test_group_by_multiple_columns() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["1_int", Type::Int, vec![3, 2, 1, 2, 2]],
        ["2_int", Type::Int, vec![4, 3, 2, 1, 3]],
        ["bool", Type::Bool, vec![true, false, true, false, true]]
    );
    let output = df.group_by(&["1_int", "2_int"]);
    assert_df_eq!(
        &pool,
        check(output),
        (1, 2, vec![true]),
        (2, 1, vec![false]),
        (2, 3, vec![false, true]),
        (3, 4, vec![true])
    );
}

#[test]
fn test_group_agg() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["bool", Type::Bool, vec![true, false, true]],
        ["int", Type::Int, vec![1, 2, 3]]
    );
    let output = || {
        df.group_by(&["bool"])?.aggregate(
            &agg!("int", Aggregator::Sum),
        )
    };
    assert_df_eq!(&pool, check(output()), (false, 2), (true, 4));
}

#[test]
fn test_agg_average() {
    let pool = Pool::new_ref();
    let df = from_vecs!(&pool, ["int", Type::Int, vec![1, 2, 3]]);
    let output = df.aggregate(&agg!("int", Aggregator::Average));
    assert_df_eq!(&pool, check(output), (2.0));
}

#[test]
fn test_agg_count() {
    let pool = Pool::new_ref();
    let df = from_vecs!(&pool, ["int", Type::Int, vec![1, 2, 3]]);
    let output = df.aggregate(&agg!("int", Aggregator::Count));
    assert_df_eq!(&pool, check(output), (3));
}

#[test]
fn test_agg_first() {
    let pool = Pool::new_ref();
    let df = from_vecs!(&pool, ["int", Type::Int, vec![1, 2, 3]]);
    let output = df.aggregate(&agg!("int", Aggregator::First));
    assert_df_eq!(&pool, check(output), (1));
}

#[test]
fn test_agg_max() {
    let pool = Pool::new_ref();
    let df = from_vecs!(&pool, ["int", Type::Int, vec![1, 2, 3, 2]]);
    let output = df.aggregate(&agg!("int", Aggregator::Max));
    assert_df_eq!(&pool, check(output), (3));
}

#[test]
fn test_agg_min() {
    let pool = Pool::new_ref();
    let df = from_vecs!(&pool, ["int", Type::Int, vec![2, 1, 2, 3]]);
    let output = df.aggregate(&agg!("int", Aggregator::Min));
    assert_df_eq!(&pool, check(output), (1));
}

#[test]
fn test_agg_sum() {
    let pool = Pool::new_ref();
    let df = from_vecs!(&pool, ["int", Type::Int, vec![1, 2, 3]]);
    let output = df.aggregate(&agg!("int", Aggregator::Sum));
    assert_df_eq!(&pool, check(output), (6));
}

#[test]
fn test_agg_multiple_columns() {
    let pool = Pool::new_ref();
    let df = from_vecs!(
        &pool,
        ["1_int", Type::Int, vec![4, 1, 6, 4, 1]],
        ["2_int", Type::Int, vec![3, 1, 1, 1, 2]],
        ["3_int", Type::Int, vec![1, 2, 3, 4, 5]]
    );
    let output = df.aggregate(&agg!(
        "1_int",
        Aggregator::Sum,
        "2_int",
        Aggregator::Min,
        "3_int",
        Aggregator::Max
    ));
    assert_df_eq!(&pool, check(output), (16, 1, 5));
}

#[test]
fn test_join() {
    let pool = Pool::new_ref();
    let left_df = from_vecs!(
        &pool,
        ["l_int", Type::Int, vec![3, 2, 1, 1]],
        ["l_str", Type::String, vec!["d", "c", "b", "a"]]
    );
    let right_df = from_vecs!(
        &pool,
        ["r_int", Type::Int, vec![3, 2, 2, 1, 1]],
        ["r_str", Type::String, vec!["i", "h", "g", "f", "e"]]
    );
    let output = left_df.join(&right_df, "l_int", "r_int");
    assert_df_eq!(&pool, check(output),
                  (1, "b", 1, "f"),
                  (1, "b", 1, "e"),
                  (1, "a", 1, "f"),
                  (1, "a", 1, "e"),
                  (2, "c", 2, "h"),
                  (2, "c", 2, "g"),
                  (3, "d", 3, "i")
    );
}

#[test]
fn test_read_csv() {
    let dir = TempDir::new("test_from_csv").unwrap();
    let path = write_csv(
        &dir,
        "example.csv",
        &["true|1|1.0|hello world", "false|4|1.2|fOObAr"],
    );
    let pool = Pool::new_ref();
    let schema = Schema::new(
        &[
            ("bool", Type::Bool),
            ("int", Type::Int),
            ("float", Type::Float),
            ("string", Type::String),
        ],
    );
    let df = DataFrame::read(&Format::Csv, path.as_path(), &schema);
    assert_df_eq!(
        &pool,
        df,
        [true, 1, 1.0, "hello world".to_string()],
        [false, 4, 1.2, "fOObAr".to_string()]
    );
}
