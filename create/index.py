import pandas as pd
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --index--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = 'index'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

indexDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
indexDistictDateList = indexDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
index = pd.read_sql_query(sql.format(tablename, indexDistictDateList[0]), conn_lite).replace('--', 0).replace('---', 0).replace('NaN', 0).fillna(0)
index.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
dateColumn = ['年月日']
varcharColumns = []
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
index.年月日 = pd.to_datetime(index.年月日).apply(lambda x: x.date()).astype(str)
index[varcharColumns] = index[varcharColumns].astype(str)
index[realColumns] = index[realColumns].astype(float)

# create table
columns = dateColumn + varcharColumns + realColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月日']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
