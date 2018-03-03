import psycopg2
import sqlite3
import pandas as pd
import numpy as np
import toolz.curried
from typing import List
import dftosql
import sqlCommand as sqlc
from update import utils


conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")
connLite = sqlite3.connect('tse.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()


table = '自營商買賣超彙總表 (股)'

columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(table), connLite))
date_columns = ['年月日']
varchar_columns = ['證券代號', '證券名稱']
integer_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
types = {'date': date_columns, 'str': varchar_columns, 'int': integer_columns}
cols_dist = ['年月日']

rows1 = toolz.compose(utils.to_dict, utils.as_type(types), sqlc.s_dist_lite(connLite, table))(cols_dist)
rows2 = toolz.compose(utils.to_dict, utils.as_type(types), sqlc.s_dist_pg(conn, table))(cols_dist)
rows = utils.diff(rows1, rows2)


@toolz.curry
def transform(dtypes: dict, df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace('--', np.nan).replace('NaN', 0).fillna(0)
    df = utils.as_type(dtypes, df)
    return df


def read_insert(row: list) -> List:
    return toolz.compose(dftosql.i_pg_batch(conn, table), transform(types), sqlc.s_where_lite(connLite, table))(row)


list(map(read_insert, rows))
