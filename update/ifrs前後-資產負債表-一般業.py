import psycopg2
import sqlite3
import pandas as pd
import numpy as np
import toolz.curried
from typing import List
import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
import dftosql
import sqlCommand as sqlc
from update import utils


conn = psycopg2.connect("host=localhost dbname=mops user=postgres password=d03724008")
connLite = sqlite3.connect('mops.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

table = 'ifrs前後-資產負債表-一般業'

columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(table), connLite))
varchar_columns = ['年', '季', '公司代號', '公司名稱']
real_columns = list(filter(lambda x: x not in varchar_columns, columns))
types = {'str': varchar_columns, 'float': real_columns}
cols_dist = ['年', '季']

rows1 = toolz.compose(utils.to_dict, utils.as_type(types), sqlc.s_dist_lite(connLite, table))(cols_dist)
rows2 = toolz.compose(utils.to_dict, utils.as_type(types), sqlc.s_dist_pg(conn, table))(cols_dist)
rows = utils.diff(rows1, rows2)


@toolz.curry
def transform(dtypes: dict, df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace('--', np.nan)
    df = utils.as_type(dtypes, df)
    return df


def read_insert(row: list) -> List:
    return toolz.compose(dftosql.i_pg_batch(conn, table), transform(types), sqlc.s_where_with_type_lite(connLite.cursor(), table, {'年':int, '季':int}))(row)


list(map(read_insert, rows))
