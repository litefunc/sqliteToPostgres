import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import sqlCommand as sqlc
import pandas as pd
import psycopg2
import sqlite3

syspath.append_if_not_exist('/home/david/program/python/project/crawler/finance/sqliteToPostgres')
import create

## --- read from sqlite ---

os.chdir(create.dbpath)

# --xdr--
# connect
conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")
connLite = sqlite3.connect('tse.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite

tablename = '除權息計算結果表'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

xdrDistictDate = pd.read_sql_query(sql.format(tablename), connLite)
xdrDistictDateList = xdrDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
xdr = pd.read_sql_query(sql.format(tablename, xdrDistictDateList[0]), connLite).rename(columns={'股票代號': '證券代號', '股票名稱': '證券名稱'})

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
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
