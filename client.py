import enum
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
    SUBTRACT = 2
    MULTIPLY = 3
    DIVIDE = 4

    def serialize(self):
        to_s = {
            ArithmeticOp.ADD: 'Add',
            ArithmeticOp.SUBTRACT: 'Subtract',
            ArithmeticOp.MULTIPLY: 'Multiply',
            ArithmeticOp.DIVIDE: 'Divide',
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

    def from_arg(arg):
        if isinstance(arg, Value):
            return ColumnExpr(ColumnExprKind.CONSTANT, arg)
        if isinstance(arg, str):
            return ColumnExpr(ColumnExprKind.SOURCE, arg)
        if isinstance(arg, (tuple, list)):
            if len(arg) == 2:
                return ColumnExpr(
                    ColumnExprKind.ALIAS,
                    arg[0],
                    ColumnExpr.from_arg(arg[1])
                )
            if len(arg) == 3:
                return ColumnExpr(
                    ColumnExprKind.OPERATION,
                    arg[0],
                    ColumnExpr.from_arg(arg[1]),
                    ColumnExpr.from_arg(arg[2])
                )

    def serialize(self):
        if len(self.args) == 1:
            arg = self.args[0]
            return {self.kind.serialize(): arg.serialize() if not isinstance(arg, str) else arg}
        return {self.kind.serialize(): [arg.serialize() if not isinstance(arg, str) else arg
                                        for arg in self.args]}


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

    def select(self, args):
        column_exprs = [
            ColumnExpr.from_arg(arg).serialize()
            for arg in args
        ]
        return Df.call(self.dataframe, {'Op': {'Select': column_exprs}})

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
             .select(['int']) \
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
                 'return_flag',
                 'line_status',
                 ('sum_qty', 'quantity'),
                 ('sum_base_price', 'extended_price'),
                 ('sum_disc_price', (ArithmeticOp.MULTIPLY, 'extended_price', (ArithmeticOp.SUBTRACT, Value(1.0), 'discount'))),
                 ('sum_charge', (ArithmeticOp.MULTIPLY, 'extended_price',
                                 (ArithmeticOp.MULTIPLY, (ArithmeticOp.SUBTRACT, Value(1.0), 'discount'), (ArithmeticOp.ADD, Value(1.0), 'tax')))),
                 ('avg_qty', 'quantity'),
                 ('avg_price', 'extended_price'),
                 ('avg_disc', 'discount'),
                 ('count_order', 'order_key'),
             ]) \
             .group_by(['return_flag', 'line_status']) \
             .order_by(['return_flag', 'line_status']) \
             .aggregate({'sum_qty': Aggregator.SUM,
                         'sum_base_price': Aggregator.SUM,
                         'sum_disc_price': Aggregator.SUM,
                         'sum_charge': Aggregator.SUM,
                         'avg_qty': Aggregator.AVERAGE,
                         'avg_price': Aggregator.AVERAGE,
                         'avg_disc': Aggregator.AVERAGE,
                         'count_order': Aggregator.COUNT}) \
             .collect()


def bench():
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
    path = 'data/line_items_full.csv'
    # .filter('ship_instructions', Predicate(Comparator.EQUAL, Value('DELIVER IN PERSON'))) \
    # .filter('order_key', Predicate(Comparator.EQUAL, Value(1))) \
    # .filter('ship_mode', Predicate(Comparator.EQUAL, Value('TRUCK'))) \
    # return Df.from_csv(path, schema) \
    #     .filter('ship_mode', Predicate(Comparator.EQUAL, Value('TRUCK'))) \
    #     .select(['quantity']) \
    #     .aggregate({'quantity': Aggregator.SUM}) \
    #     .collect()
    return Df.from_csv(path, schema) \
        .select(['ship_mode']) \
        .order_by(['ship_mode']) \
        .aggregate({'ship_mode': Aggregator.FIRST}) \
        .collect()
