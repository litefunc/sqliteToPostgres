import pandas as pd
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --大盤成交統計--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '大盤成交統計'
sql = "SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

dfDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
dfDistictDateList = dfDistictDate['年月日'].tolist()

sql = 'SELECT * FROM "{}" WHERE "年月日"="{}"'
df = pd.read_sql_query(sql.format(tablename, dfDistictDateList[-1]), conn_lite).replace('--', 0).replace('NaN', 0).fillna(0)

df.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
dateColumn = ['年月日']
varcharColumns = ['成交統計']
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
df[varcharColumns] = df[varcharColumns].astype(str)
df[realColumns] = df[realColumns].astype(float)

# create table
columns = dateColumn + varcharColumns + realColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(16)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月日', '成交統計']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
