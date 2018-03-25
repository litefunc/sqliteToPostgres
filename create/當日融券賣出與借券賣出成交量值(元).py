import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --margin--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '當日融券賣出與借券賣出成交量值(元)'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

marginDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
marginDistictDateList = marginDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
margin = pd.read_sql_query(sql.format(tablename, marginDistictDateList[0]), conn_lite).replace('--', np.nan)

margin.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
dateColumn = ['年月日']
varcharColumns = ['證券代號', '證券名稱']
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
margin.年月日 = pd.to_datetime(margin.年月日).apply(lambda x: x.date()).astype(str)
margin[varcharColumns] = margin[varcharColumns].astype(str)
margin[realColumns] = margin[realColumns].astype(float)

# create table
columns = dateColumn + varcharColumns + realColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
