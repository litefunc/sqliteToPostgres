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

# --compositeIndex--
# connect
conn = psycopg2.connect("host=localhost dbname=bic user=postgres password=d03724008")
connLite = sqlite3.connect('bic.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite

tablename = '景氣指標及燈號-綜合指數'
sql="SELECT DISTINCT `年月` FROM '{}'".format(tablename)

compositeIndexDistictDate = pd.read_sql_query(sql.format(tablename), connLite)
compositeIndexDistictDateList = compositeIndexDistictDate['年月'].tolist()

sql="SELECT * FROM '{}' WHERE 年月 = '{}'"
compositeIndex = pd.read_sql_query(sql.format(tablename, compositeIndexDistictDateList[0]), connLite).replace('--', 0).replace('NaN', 0).fillna(0)

compositeIndex.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), connLite))
dateColumn = []
varcharColumns = ['年月', '年', '月']
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
compositeIndex[varcharColumns] = compositeIndex[varcharColumns].astype(str)
compositeIndex[realColumns] = compositeIndex[realColumns].astype(float)

# create table
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
