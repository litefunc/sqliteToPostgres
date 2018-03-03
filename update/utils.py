import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
syspath.append_if_not_exist('/home/david/program/python/project/crawler/finance/sqliteToPostgres')

# db_path = '/home/david/Documents/db/'
db_path = os.getenv('SQLITE_DB')
os.chdir(db_path)

# from update import init
import psycopg2
import sqlCommand as sqlc

import numpy as np
import pandas as pd
import toolz.curried
import datetime
from astype.astype import *
from astype.astype import to_date, to_string, to_dict, as_type

def identity(x):
    return x


@toolz.curry
def diff(x, y):
    return [key for key in x if key not in y]


def update(table, cols, dateColumn=[], varcharColumns=[], realColumns=[], transform=identity):
    sql1 = 'SELECT distinct {} FROM "{}"'.format(sqlc.quoteToString("`", cols), table)
    df1 = to_date(pd.read_sql_query(sql1, connLite), dateColumn)  # in order to convert %y/%m/%d to %y-%m-%d
    df1 = to_string(df1, varcharColumns)
    rows1 = to_string(df1, dateColumn).to_dict('records')

    sql2 = 'SELECT distinct {} FROM "{}"'.format(sqlc.quoteToString('"', cols), table)
    df2 = to_date(pd.read_sql_query(sql2, conn), dateColumn)
    df2 = to_string(df2, varcharColumns)
    rows2 = to_string(df2, dateColumn).to_dict('records')

    rows = [key for key in rows1 if key not in rows2]

    for row in rows:
        sql = 'SELECT * FROM "{}" where {}'.format(table, join(' and ', ['"{0}"="{1}"'.format(key, value) for key, value in row.items()]))
        df = pd.read_sql_query(sql, connLite)
        sqlc.insertDataPostgre(table, transform(dateColumn, varcharColumns, realColumns, df), conn)
