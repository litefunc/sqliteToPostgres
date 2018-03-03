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

#---- bic ----

# --bic--
# connect
conn = psycopg2.connect("host=localhost dbname=bic user=postgres password=d03724008")
connLite = sqlite3.connect('bic.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite

tablename = '景氣指標及燈號-指標構成項目'
sql="SELECT DISTINCT `年月` FROM '{}'".format(tablename)

bicDistictDate = pd.read_sql_query(sql.format(tablename), connLite)
bicDistictDateList = bicDistictDate['年月'].tolist()

sql="SELECT * FROM '{}' WHERE 年月 = '{}'"
bic = pd.read_sql_query(sql.format(tablename, bicDistictDateList[0]), connLite).rename(columns={'工業及服務業受僱員工淨進入率(%)': '工業及服務業受僱員工淨進入率','失業率(%)': '失業率', '製造業存貨率(%)': '製造業存貨率'}).replace('--', 0).replace('NaN', 0).fillna(0)

bic.dtypes
columns = list(bic)
dateColumn = []
varcharColumns = ['年月', '年', '月']
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
bic[varcharColumns] = bic[varcharColumns].astype(str)
bic[realColumns] = bic[realColumns].astype(float)

# create table
columns = dateColumn + varcharColumns + realColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
