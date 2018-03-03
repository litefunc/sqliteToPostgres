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

# --value--
# connect
conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")
connLite = sqlite3.connect('tse.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite

tablename = '個股日本益比、殖利率及股價淨值比'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

valueDistictDate = pd.read_sql_query(sql.format(tablename), connLite)
valueDistictDateList = valueDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
value = pd.read_sql_query(sql.format(tablename, valueDistictDateList[0]), connLite).replace('--', np.nan) # column name can not contain % otherwise data can't be inserted
value['本益比'] = value['本益比'].replace('-', 0).replace('NaN', 0).fillna(0)
value['股價淨值比'] = value['股價淨值比'].replace('-', 0).replace('NaN', 0).fillna(0)

value.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), connLite))
dateColumn = ['年月日']
varcharColumns = ['證券代號', '證券名稱', '財報年/季']
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
value.年月日 = pd.to_datetime(value.年月日).apply(lambda x: x.date()).astype(str)
value[varcharColumns] = value[varcharColumns].astype(str)
value[realColumns] = value[realColumns].astype(float)

# create table
columns = dateColumn + varcharColumns + realColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
