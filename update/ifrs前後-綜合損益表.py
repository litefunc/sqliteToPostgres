import psycopg2
import sqlite3
import pandas as pd
import numpy as np
import toolz.curried
from typing import List
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import dftosql
import sqlCommand as sqlc
from sqliteToPostgres.update import utils


# --inc--
# connect
conn_lite = conn_local_lite('mops.sqlite3')
conn_pg = conn_local_pg('mops')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()


table = 'ifrs前後-綜合損益表'


columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(table), conn_lite))
varchar_columns = ['年', '季', '公司代號', '公司名稱']
real_columns = list(filter(lambda x: x not in varchar_columns, columns))
types = {'str': varchar_columns, 'float': real_columns}
cols_dist = ['年', '季']
rows1 = toolz.compose(utils.to_dict, utils.as_type(types), sqlc.s_dist_lite(conn_lite, table))(cols_dist)
rows2 = toolz.compose(utils.to_dict, utils.as_type(types), sqlc.s_dist_pg(conn_pg, table))(cols_dist)
rows = utils.diff(rows1, rows2)


@toolz.curry
def transform(types: dict, df: pd.DataFrame) -> pd.DataFrame:
    df = df.replace('--', np.nan)
    df = utils.as_type(types, df)
    print(df)
    return df

def read_insert(row: list) -> List:
    return toolz.compose(dftosql.i_pg_batch(conn_pg, table), transform(types), sqlc.s_where_with_type_lite(conn_lite.cursor(), table, {'年':int, '季':str}))(row)

list(map(read_insert, rows))
