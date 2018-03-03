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

# --index--
# connect
conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")
connLite = sqlite3.connect('tse.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite

tablename = 'index'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

indexDistictDate = pd.read_sql_query(sql.format(tablename), connLite)
indexDistictDateList = indexDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
index = pd.read_sql_query(sql.format(tablename, indexDistictDateList[0]), connLite).replace('--', 0).replace('---', 0).replace('NaN', 0).fillna(0)
index.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), connLite))
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
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
