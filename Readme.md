# DF

Columnar in-memory DataFrames in Rust

### Install

```
$ cargo build

# In a python 3 virtualenv
$ pip install ipython
```

### Server and Python client

In one shell start the server

```
$ cargo run --release server
```

And in another shell connect to it with the Python client

```
$ ipython -i client.py
```

Then you can interact with the server using the Df class

```python
schema = Schema([('int', Type.INT),
                 ('string', Type.STRING),
                 ('boolean', Type.BOOLEAN)])

df = Df.from_csv('data/small.csv', schema) \
       .filter('boolean', Predicate(Comparator.EQUAL, Value(True))) \
       .select(['int']) \
       .aggregate({'int': Aggregator.AVERAGE})

df.collect()
```

### Running the tests

```
$ RUST_BACKTRACE=1 cargo test --test lib
```
