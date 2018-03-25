import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --trust--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '投信買賣超彙總表 (股)'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

trustDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
trustDistictDateList = trustDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
trust = pd.read_sql_query(sql.format(tablename, trustDistictDateList[0]), conn_lite).rename(columns={'買進股數':'投信買進股數','賣出股數':'投信賣出股數','買賣超股數':'投信買賣超股數','鉅額交易': '投信鉅額交易'}).replace('--', np.nan).replace('NaN', 0).fillna(0)
trust[['投信鉅額交易']] = trust[['投信鉅額交易']].applymap(lambda x:0 if x == ' ' else 1)

trust.dtypes
columns = list(trust)
dateColumn = ['年月日']
varcharColumns = ['證券代號', '證券名稱']
integerColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
trust.年月日 = pd.to_datetime(trust.年月日).apply(lambda x: x.date()).astype(str)
trust[varcharColumns] = trust[varcharColumns].astype(str)
trust[integerColumns] = trust[integerColumns].astype(int)

# create table
columns = dateColumn + varcharColumns + integerColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['integer' for col in integerColumns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
