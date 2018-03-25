import pandas as pd
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --xdr--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '除權息計算結果表'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

xdrDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
xdrDistictDateList = xdrDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
xdr = pd.read_sql_query(sql.format(tablename, xdrDistictDateList[0]), conn_lite).rename(columns={'股票代號': '證券代號', '股票名稱': '證券名稱'})

xdr.dtypes
columns = list(xdr)
dateColumn = ['年月日']
varcharColumns = ['證券代號', '證券名稱']
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
xdr.年月日 = pd.to_datetime(xdr.年月日).apply(lambda x: x.date()).astype(str)
xdr[varcharColumns] = xdr[varcharColumns].astype(str)
xdr[realColumns] = xdr[realColumns].astype(float)

# create table
columns = dateColumn + varcharColumns + realColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
