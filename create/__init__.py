import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import psycopg2
import sqlCommand as sqlCommand

import sqlite3
import numpy as np
import pandas as pd
import cytoolz.curried


from datetime import datetime, timedelta
from copy import deepcopy

from bs4 import BeautifulSoup

from functools import *


pd.get_option("display.max_rows")
pd.get_option("display.max_columns")
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", 1000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.unicode.east_asian_width', True)


def join(s, l):
    return s.join(l)

@cytoolz.curry
def tostring(cols, df):
    l = list(df)
    for col in cols:
        if col in l:
            df[[col]] = df[[col]].astype(str)
    return df

@cytoolz.curry
def toDate(cols, df):
    l = list(df)
    for col in cols:
        if col in l:
            df[col] = pd.to_datetime(df[col]).apply(lambda x: x.date())
    return df

def identity(x):
    return x

def toDict(df):
    return df.to_dict('records')

@cytoolz.curry
def selectDistinctSqlite(conn, tablename: str, cols):
    sql1 = 'SELECT distinct {} FROM "{}"'.format(sqlCommand.quoteToString("`", cols), tablename)
    return pd.read_sql_query(sql1, conn)

@cytoolz.curry
def selectDistinctPostgres(conn, tablename: str, cols):
    sql1 = 'SELECT distinct {} FROM "{}"'.format(sqlCommand.quoteToString('"', cols), tablename)
    return pd.read_sql_query(sql1, conn)

@cytoolz.curry
def selectWhereSqlite(conn, tablename: str, row):
    sql = 'SELECT * FROM "{}" where {}'.format(tablename, join(' and ',['"{0}"="{1}"'.format(key, value) for key, value in row.items()]))
    return pd.read_sql_query(sql, conn)


@cytoolz.curry
def difference(x, y):
    return [key for key in x if key not in y]


dbpath = '/home/david/Documents/db/'
