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

# --margin--
# connect
conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")
connLite = sqlite3.connect('tse.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite

tablename = '當日融券賣出與借券賣出成交量值(元)'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

marginDistictDate = pd.read_sql_query(sql.format(tablename), connLite)
marginDistictDateList = marginDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
margin = pd.read_sql_query(sql.format(tablename, marginDistictDateList[0]), connLite).replace('--', np.nan)

margin.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), connLite))
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
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
