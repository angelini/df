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
             .select(['int']) \
             .collect()
