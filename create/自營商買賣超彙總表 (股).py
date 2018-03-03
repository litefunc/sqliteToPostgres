import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import sqlCommand as sqlc
import pandas as pd
import numpy as np
import psycopg2
import sqlite3

syspath.append_if_not_exist('/home/david/program/python/project/crawler/finance/sqliteToPostgres')
import create

## --- read from sqlite ---

os.chdir(create.dbpath)

# --deal--
# connect
conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")
connLite = sqlite3.connect('tse.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite

tablename = '自營商買賣超彙總表 (股)'
sql="SELECT DISTINCT `年月日` FROM `{}`".format(tablename)

dealDistictDate = pd.read_sql_query(sql.format(tablename), connLite)
dealDistictDateList = dealDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
deal = pd.read_sql_query(sql.format(tablename, dealDistictDateList[0]), connLite).replace('--', np.nan).replace('NaN', 0).fillna(0)
deal[['自營商(自行買賣)賣出股數', '自營商(自行買賣)買賣超股數', '自營商(自行買賣)買進股數', '自營商(避險)賣出股數', '自營商(避險)買賣超股數', '自營商(避險)買進股數', '自營商賣出股數', '自營商買賣超股數', '自營商買進股數']] = deal[['自營商(自行買賣)賣出股數', '自營商(自行買賣)買賣超股數', '自營商(自行買賣)買進股數', '自營商(避險)賣出股數', '自營商(避險)買賣超股數', '自營商(避險)買進股數', '自營商賣出股數', '自營商買賣超股數', '自營商買進股數']].fillna(0)

deal.dtypes
columns = list(pd.read_sql_query("SELECT * FROM `{}` limit 1".format(tablename), connLite))
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
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
