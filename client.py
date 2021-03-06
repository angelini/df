import enum
import json
import requests

URI = 'http://127.0.0.1:3000/call'


class Type(enum.Enum):
    BOOL = 1
    INT = 2
    FLOAT = 3
    STRING = 4

    def serialize(self):
        to_s = {
            Type.BOOL: 'Bool',
            Type.INT: 'Int',
            Type.FLOAT: 'Float',
            Type.STRING: 'String'
        }
        return to_s[self]



class Schema:

    def __init__(self, columns):
        self.columns = columns

    def serialize(self):
        return {'columns': [{'name': col[0], 'type_': col[1].serialize()}
                            for col in self.columns]}


class Comparator(enum.Enum):
    EQUAL = 1
    GREATER_THAN = 2
    GREATER_THAN_OR_EQ = 3
    LESS_THAN = 4
    LESS_THAN_OR_EQ = 5

    def serialize(self):
        to_s = {
            Comparator.EQUAL: 'Equal',
            Comparator.GREATER_THAN: 'GreaterThan',
            Comparator.GREATER_THAN_OR_EQ: 'GreaterThanOrEq',
            Comparator.LESS_THAN: 'LessThan',
            Comparator.LESS_THAN_OR_EQ: 'LessThanOrEq',
        }
        return to_s[self]


class Value:

    def __init__(self, val):
        self.val = val

    def serialize(self):
        to_s = {
            bool: 'Bool',
            int: 'Int',
            float: 'Float',
            str: 'String',
        }
        if isinstance(self.val, float):
            val = {'value': self.val, 'phantom': None}
        else:
            val = self.val
        return {to_s[type(self.val)]: val}


class Predicate:

    def __init__(self, comparator, value):
        self.comparator = comparator
        self.value = value

    def serialize(self):
        return {'comparator': self.comparator.serialize(),
                'value': self.value.serialize()}


class Aggregator(enum.Enum):
    AVERAGE = 1
    COUNT = 2
    FIRST = 3
    SUM = 4
    MAX = 5
    MIN = 6

    def serialize(self):
        to_s = {
            Aggregator.AVERAGE: 'Average',
            Aggregator.COUNT: 'Count',
            Aggregator.FIRST: 'First',
            Aggregator.SUM: 'Sum',
            Aggregator.MAX: 'Max',
            Aggregator.MIN: 'Min',
        }
        return to_s[self]


class ArithmeticOp(enum.Enum):
    ADD = 1
    SUB = 2
    MUL = 3
    DIV = 4

    def serialize(self):
        to_s = {
            ArithmeticOp.ADD: 'Add',
            ArithmeticOp.SUB: 'Subtract',
            ArithmeticOp.MUL: 'Multiply',
            ArithmeticOp.DIV: 'Divide',
        }
        return to_s[self]


class ColumnExprKind(enum.Enum):
    CONSTANT = 1
    SOURCE = 2
    ALIAS = 3
    OPERATION = 4

    def serialize(self):
        to_s = {
            ColumnExprKind.CONSTANT: 'Constant',
            ColumnExprKind.SOURCE: 'Source',
            ColumnExprKind.ALIAS: 'Alias',
            ColumnExprKind.OPERATION: 'Operation',
        }
        return to_s[self]


class ColumnExpr:

    def __init__(self, kind, *args):
        self.kind = kind
        self.args = args

    def __add__(self, other):
        return ColumnExpr(
            ColumnExprKind.OPERATION,
            ArithmeticOp.ADD,
            self,
            other
        )

    def __sub__(self, other):
        return ColumnExpr(
            ColumnExprKind.OPERATION,
            ArithmeticOp.SUB,
            self,
            other
        )

    def __mul__(self, other):
        return ColumnExpr(
            ColumnExprKind.OPERATION,
            ArithmeticOp.MUL,
            self,
            other
        )

    def __truediv__(self, other):
        return ColumnExpr(
            ColumnExprKind.OPERATION,
            ArithmeticOp.DIV,
            self,
            other
        )

    def alias(self, name):
        return ColumnExpr(
            ColumnExprKind.ALIAS,
            name,
            self
        )

    def serialize(self):
        if len(self.args) == 1:
            arg = self.args[0]
            return {self.kind.serialize(): arg.serialize() if not isinstance(arg, str) else arg}
        return {self.kind.serialize(): [arg.serialize() if not isinstance(arg, str) else arg
                                        for arg in self.args]}


def c(expr):
    if isinstance(expr, Value):
        return ColumnExpr(ColumnExprKind.CONSTANT, expr)
    return ColumnExpr(ColumnExprKind.SOURCE, expr)


class Df:

    def __init__(self, dataframe, values):
        self.dataframe = dataframe
        self.values = values

    @staticmethod
    def call(dataframe, function):
        print(function)
        res = requests.post(URI, json={'dataframe': dataframe,
                                       'function': function})
        if not res.ok:
            raise ValueError(res.content)
        json = res.json()
        return Df(json['dataframe'], json['blocks'])

    @staticmethod
    def from_csv(path, schema):
        return Df.call(None, {'Read': ['csv', path, schema.serialize()]})

    def select(self, column_exprs):
        serialized = [expr.serialize() for expr in column_exprs]
        return Df.call(self.dataframe, {'Op': {'Select': serialized}})

    def filter(self, column_name, predicate):
        return Df.call(self.dataframe, {'Op': {'Filter': [column_name,
                                                          predicate.serialize()]}})

    def order_by(self, column_names):
        return Df.call(self.dataframe, {'Op': {'OrderBy': column_names}})

    def group_by(self, column_names):
        return Df.call(self.dataframe, {'Op': {'GroupBy': column_names}})

    def aggregate(self, aggregators):
        serialized = {col: agg.serialize() for (col, agg) in aggregators.items()}
        return Df.call(self.dataframe, {'Op':
                                        {'Aggregation': serialized}})

    def join(self, right, left_col, right_col):
        return Df.call(self.dataframe, {'Op': {'Join': [right.dataframe,
                                                        left_col,
                                                        right_col]}})

    def collect(self):
        return Df.call(self.dataframe, {'Action': 'Collect'}).values

    def count(self):
        return Df.call(self.dataframe, {'Action': 'Count'}).values

    def take(self, n):
        return Df.call(self.dataframe, {'Action': {'Take': n}}).values


def example_small():
    schema = Schema([('int', Type.INT),
                     ('string', Type.STRING),
                     ('bool', Type.BOOL)])
    return Df.from_csv('data/small.csv', schema) \
             .filter('bool', Predicate(Comparator.EQUAL, Value(True))) \
             .select([c('int')]) \
             .aggregate({'int': Aggregator.AVERAGE}) \
             .collect()


def example_line_items(full=False):
    """
    -- $ID$
    -- TPC-H/TPC-R Pricing Summary Report Query (Q1)
    -- Functional Query Definition
    -- Approved February 1998
    :x
    :o
    select
        l_returnflag,
        l_linestatus,
        sum(l_quantity) as sum_qty,
        sum(l_extendedprice) as sum_base_price,
        sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
        sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
        avg(l_quantity) as avg_qty,
        avg(l_extendedprice) as avg_price,
        avg(l_discount) as avg_disc,
        count(*) as count_order
    from
        lineitem
    where
        l_shipdate <= date '1998-12-01' - interval ':1' day (3)
    group by
        l_returnflag,
        l_linestatus
    order by
        l_returnflag,
        l_linestatus;
    :n -1
    """
    schema = Schema([('order_key', Type.INT),
                     ('part_key', Type.INT),
                     ('supplier_key', Type.INT),
                     ('line_number', Type.INT),
                     ('quantity', Type.FLOAT),
                     ('extended_price', Type.FLOAT),
                     ('discount', Type.FLOAT),
                     ('tax', Type.FLOAT),
                     ('return_flag', Type.STRING),
                     ('line_status', Type.STRING),
                     ('ship_date', Type.STRING),
                     ('commit_date', Type.STRING),
                     ('receipt_date', Type.STRING),
                     ('ship_instructions', Type.STRING),
                     ('ship_mode', Type.STRING),
                     ('comment', Type.STRING)])
    path = 'data/line_items{}.csv'.format('_full' if full else '')
    return Df.from_csv(path, schema) \
             .filter('ship_date', Predicate(Comparator.LESS_THAN_OR_EQ, Value('1998-12-01'))) \
             .select([
                 c('return_flag'),
                 c('line_status'),
                 c('quantity').alias('sum_qty'),
                 c('extended_price').alias('sum_base_price'),
                 ((c(Value(1.0)) - c('discount')) * c('extended_price')).alias('sum_disc_price'),
                 (((c(Value(1.0)) + c('tax')) * (c(Value(1.0)) - c('discount'))) * c('extended_price')).alias('sum_charge'),
                 c('quantity').alias('avg_quantity'),
                 c('extended_price').alias('avg_price'),
                 c('discount').alias('avg_discount'),
                 c('order_key').alias('count_order')
             ]) \
             .group_by(['return_flag', 'line_status']) \
             .order_by(['return_flag', 'line_status']) \
             .aggregate({'sum_qty': Aggregator.SUM,
                         'sum_base_price': Aggregator.SUM,
                         'sum_disc_price': Aggregator.SUM,
                         'sum_charge': Aggregator.SUM,
                         'avg_quantity': Aggregator.AVERAGE,
                         'avg_price': Aggregator.AVERAGE,
                         'avg_discount': Aggregator.AVERAGE,
                         'count_order': Aggregator.COUNT}) \
             .collect()
