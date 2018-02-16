import enum
import requests

URI = 'http://127.0.0.1:3000/call'


class Type(enum.Enum):
    BOOLEAN = 1
    INT = 2
    FLOAT = 3
    STRING = 4

    def serialize(self):
        to_s = {
            Type.BOOLEAN: 'Boolean',
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
            bool: 'Boolean',
            int: 'Int',
            float: 'Float',
            str: 'String',
        }
        return {to_s[type(self.val)]: self.val}


class Predicate:

    def __init__(self, comparator, value):
        self.comparator = comparator
        self.value = value

    def serialize(self):
        return {'comparator': self.comparator.serialize(),
                'value': self.value.serialize()}


class Aggregator(enum.Enum):
    FIRST = 1
    SUM = 2
    MAX = 3
    MIN = 4

    def serialize(self):
        to_s = {
            Aggregator.FIRST: 'First',
            Aggregator.SUM: 'Sum',
            Aggregator.MAX: 'Max',
            Aggregator.MIN: 'Min',
        }
        return to_s[self]


class Df:

    def __init__(self, dataframe, values):
        self.dataframe = dataframe
        self.values = values

    @staticmethod
    def call(dataframe, function):
        res = requests.post(URI, json={'dataframe': dataframe,
                                       'function': function})
        if not res.ok:
            raise ValueError(res.content)
        json = res.json()
        return Df(json['dataframe'], json['values'])

    @staticmethod
    def from_csv(path, schema):
        return Df.call(None, {'Read': ['csv', path, schema.serialize()]})

    def select(self, column_names):
        return Df.call(self.dataframe, {'Op': {'Select': column_names}})

    def filter(self, column_name, predicate):
        return Df.call(self.dataframe, {'Op': {'Filter': [column_name,
                                                          predicate.serialize()]}})

    def order_by(self, column_names):
        return Df.call(self.dataframe, {'Op': {'OrderBy': column_names}})

    def group_by(self, column_names):
        return Df.call(self.dataframe, {'Op': {'GroupBy': column_names}})

    def aggregate(self, aggregators):
        serialized = {col: agg.serialize() for (col, agg) in aggregators.items()}
        print('serialize {}'.format(serialized))
        return Df.call(self.dataframe, {'Op':
                                        {'Aggregation': serialized}})

    def collect(self):
        return Df.call(self.dataframe, {'Action': 'Collect'}).values

    def count(self):
        return Df.call(self.dataframe, {'Action': 'Count'}).values

    def take(self, n):
        return Df.call(self.dataframe, {'Action': {'Take': n}}).values


def example():
    schema = Schema([('int', Type.INT),
                     ('string', Type.STRING),
                     ('boolean', Type.BOOLEAN)])
    return Df.from_csv('data/small.csv', schema) \
             .filter('boolean', Predicate(Comparator.EQUAL, Value(True))) \
             .select(['int']) \
             .aggregate({'int': Aggregator.SUM}) \
             .collect()
