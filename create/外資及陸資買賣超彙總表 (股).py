import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --fore--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '外資及陸資買賣超彙總表 (股)'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

foreDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
foreDistictDateList = foreDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
fore = pd.read_sql_query(sql.format(tablename, foreDistictDateList[0]), conn_lite).rename(columns={'買進股數':'外資買進股數','賣出股數':'外資賣出股數','買賣超股數':'外資買賣超股數','鉅額交易': '外資鉅額交易'}).replace('--', np.nan).replace('NaN', 0).fillna(0)
fore[['外資鉅額交易']] = fore[['外資鉅額交易']].applymap(lambda x:0 if x == ' ' else 1)

set(fore.外資鉅額交易)
fore.dtypes
columns = list(fore)
dateColumn = ['年月日']
varcharColumns = ['證券代號', '證券名稱']
integerColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
fore.年月日 = pd.to_datetime(fore.年月日).apply(lambda x: x.date()).astype(str)
fore[varcharColumns] = fore[varcharColumns].astype(str)
fore[integerColumns] = fore[integerColumns].astype(int)

# create table
columns = dateColumn + varcharColumns + integerColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['integer' for col in integerColumns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
