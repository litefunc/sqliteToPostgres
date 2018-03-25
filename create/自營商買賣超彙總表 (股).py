import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --deal--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '自營商買賣超彙總表 (股)'
sql="SELECT DISTINCT `年月日` FROM `{}`".format(tablename)

dealDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
dealDistictDateList = dealDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
deal = pd.read_sql_query(sql.format(tablename, dealDistictDateList[0]), conn_lite).replace('--', np.nan).replace('NaN', 0).fillna(0)
deal[['自營商(自行買賣)賣出股數', '自營商(自行買賣)買賣超股數', '自營商(自行買賣)買進股數', '自營商(避險)賣出股數', '自營商(避險)買賣超股數', '自營商(避險)買進股數', '自營商賣出股數', '自營商買賣超股數', '自營商買進股數']] = deal[['自營商(自行買賣)賣出股數', '自營商(自行買賣)買賣超股數', '自營商(自行買賣)買進股數', '自營商(避險)賣出股數', '自營商(避險)買賣超股數', '自營商(避險)買進股數', '自營商賣出股數', '自營商買賣超股數', '自營商買進股數']].fillna(0)

deal.dtypes
columns = list(pd.read_sql_query("SELECT * FROM `{}` limit 1".format(tablename), conn_lite))
dateColumn = ['年月日']
varcharColumns = ['證券代號', '證券名稱']
integerColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
deal.年月日 = pd.to_datetime(deal.年月日).apply(lambda x: x.date()).astype(str)
deal[varcharColumns] = deal[varcharColumns].astype(str)
deal[integerColumns] = deal[integerColumns].astype(int)

# create table
columns = dateColumn + varcharColumns + integerColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['integer' for col in integerColumns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
