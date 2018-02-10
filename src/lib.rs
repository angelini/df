#![feature(box_syntax, box_patterns, plugin, conservative_impl_trait, universal_impl_trait)]

#![plugin(clippy)]

extern crate decorum;
extern crate rand;

pub mod aggregate;
pub mod dataframe;
pub mod pool;
pub mod value;

#[macro_export]
macro_rules! agg {
    ( $( $c:expr, $a:expr ),* ) => {{
        let mut aggregators = std::collections::BTreeMap::new();
        $(
            aggregators.insert($c.to_string(), $a);
        )*
        aggregators
    }};
}

#[macro_export]
macro_rules! predicate {
    ( == $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::Equal, df::value::Value::from($v))
    }};
    ( > $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::GreaterThan, df::value::Value::from($v))
    }};
    ( >= $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::GreaterThanOrEq, df::value::Value::from($v))
    }};
    ( < $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::LessThan, df::value::Value::from($v))
    }};
    ( <= $v:expr ) => {{
        df::value::Predicate::new(df::value::Comparator::LessThanOrEq, df::value::Value::from($v))
    }};
}

#[macro_export]
macro_rules! from_vecs {
    ( $p:expr, $( ($n:expr, $t:path, $v:expr) ),* ) => {{
        let schema = df::dataframe::Schema::new(
            &[ $( $n, )* ],
            &[ $( $t, )* ],
        );
        let mut values = std::collections::HashMap::new();
        $(
            values.insert($n.to_string(), df::value::Values::from($v));
        )*
        df::dataframe::DataFrame::new($p, schema, values)
    }};
    ( $p:expr, $( ($n:expr, $t:path, $v:expr,) ),* ) => {{
        from_vecs!($p, $(($n, $t, $v)),*)
    }};
}
