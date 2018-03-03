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

# --trust--
# connect
conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")
connLite = sqlite3.connect('tse.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite

tablename = '投信買賣超彙總表 (股)'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

trustDistictDate = pd.read_sql_query(sql.format(tablename), connLite)
trustDistictDateList = trustDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
trust = pd.read_sql_query(sql.format(tablename, trustDistictDateList[0]), connLite).rename(columns={'買進股數':'投信買進股數','賣出股數':'投信賣出股數','買賣超股數':'投信買賣超股數','鉅額交易': '投信鉅額交易'}).replace('--', np.nan).replace('NaN', 0).fillna(0)
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
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
