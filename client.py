from enum import Enum
import requests

URI = 'http://127.0.0.1:3000/call'


class Type(Enum):
    BOOLEAN = 1
    INT = 2
    FLOAT = 3
    STRING = 4


def type_(t):
    to_s = {
        Type.BOOLEAN: 'Boolean',
        Type.INT: 'Int',
        Type.FLOAT: 'Float',
        Type.STRING: 'String'
    }
    return to_s[t]


def collect():
    return {'Action': 'Collect'}


def count():
    return {'Action': 'Count'}


def take(n):
    return {'Action': {'Take': n}}


def from_csv(path, schema):
    return {'Read': ['csv', path, schema]}


def schema(columns):
    return {'columns': [{'name': col[0], 'type_': col[1]}
                        for col in columns]}


def select(column_names):
    return {'Op': {'Select': column_names}}


def call(dataframe, function):
    print('df: {}'.format(dataframe))
    print('fn: {}'.format(function))
    res = requests.post(URI, json={'dataframe': dataframe,
                                   'function': function})
    if res.ok:
        return res.json()
    return res.content


def example():
    res = call(None, from_csv('data/small.csv',
                              schema([('int', type_(Type.INT)),
                                      ('string', type_(Type.STRING)),
                                      ('boolean', type_(Type.BOOLEAN))])))
    selected = call(res['dataframe'], select(['int']))
    return call(selected['dataframe'], collect())
