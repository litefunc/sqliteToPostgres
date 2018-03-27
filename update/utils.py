import numpy as np
import pandas as pd
import cytoolz.curried
import datetime
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))
    
import syspath

db_path = os.getenv('SQLITE_DB')
os.chdir(db_path)


import sqlCommand as sqlc
from astype.astype import *
from astype.astype import to_date, to_string, to_dict, as_type
from common.connection import conn_local_lite, conn_local_pg

conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

def identity(x):
    return x


@cytoolz.curry
def diff(x, y):
    return [key for key in x if key not in y]


def update(table, cols, dateColumn=[], varcharColumns=[], realColumns=[], transform=identity):
    sql1 = 'SELECT distinct {} FROM "{}"'.format(sqlc.quoteToString("`", cols), table)
    df1 = to_date(pd.read_sql_query(sql1, conn_lite), dateColumn)  # in order to convert %y/%m/%d to %y-%m-%d
    df1 = to_string(df1, varcharColumns)
    rows1 = to_string(df1, dateColumn).to_dict('records')

    sql2 = 'SELECT distinct {} FROM "{}"'.format(sqlc.quoteToString('"', cols), table)
    df2 = to_date(pd.read_sql_query(sql2, conn_pg), dateColumn)
    df2 = to_string(df2, varcharColumns)
    rows2 = to_string(df2, dateColumn).to_dict('records')

    rows = [key for key in rows1 if key not in rows2]

    for row in rows:
        sql = 'SELECT * FROM "{}" where {}'.format(table, join(' and ', ['"{0}"="{1}"'.format(key, value) for key, value in row.items()]))
        df = pd.read_sql_query(sql, conn_lite)
        sqlc.insertDataPostgre(table, transform(dateColumn, varcharColumns, realColumns, df), conn_pg)
