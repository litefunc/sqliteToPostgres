import psycopg2
import sqlite3
import pandas as pd
import numpy as np
import toolz.curried
from typing import List
import dftosql
import sqlCommand as sqlc
from update import utils


conn = psycopg2.connect("host=localhost dbname=bic user=postgres password=d03724008")
connLite = sqlite3.connect('bic.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()


table = '景氣指標及燈號-綜合指數'

columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(table), connLite))
date_columns = []
varchar_columns = ['年月', '年', '月']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
types = {'date': date_columns, 'str': varchar_columns, 'float': real_columns}
cols_dist = ['年月']

rows1 = toolz.compose(utils.to_dict, utils.as_type(types), sqlc.s_dist_lite(connLite, table))(cols_dist)
rows2 = toolz.compose(utils.to_dict, utils.as_type(types), sqlc.s_dist_pg(conn, table))(cols_dist)
rows = utils.diff(rows1, rows2)


@toolz.curry
def transform(dtypes: dict, df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace('--', 0).replace('NaN', 0).fillna(0)
    df = utils.as_type(dtypes, df)
    return df


def read_insert(row: list) -> List:
    return toolz.compose(dftosql.i_pg_batch(conn, table), transform(types), sqlc.s_where_lite(connLite, table))(row)


list(map(read_insert, rows))
