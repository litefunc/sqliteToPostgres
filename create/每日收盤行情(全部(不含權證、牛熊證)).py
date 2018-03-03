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

#--- tse ---

# --close--
# connect
conn = psycopg2.connect("host=localhost dbname=tse user=postgres password=d03724008")
connLite = sqlite3.connect('tse.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite

tablename = '每日收盤行情(全部(不含權證、牛熊證))'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

closeDistictDate = pd.read_sql_query(sql.format(tablename), connLite)
closeDistictDateList = closeDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" where "年月日"="{}"'
close = pd.read_sql_query(sql.format(tablename, closeDistictDateList[0]), connLite)
close['漲跌(+/-)'] = close['漲跌(+/-)'].replace('＋', 1).replace('－', -1).replace('X', 0).replace(' ', 0).astype(float)
close['本益比'] = close['本益比'].replace('NaN', 0).fillna(0)

close.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), connLite))
dateColumn = ['年月日']
varcharColumns = ['證券代號', '證券名稱']
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
close.年月日 = pd.to_datetime(close.年月日).apply(lambda x: x.date()).astype(str)

close[varcharColumns] = close[varcharColumns].astype(str)
close[realColumns] = close[realColumns].astype(float)

# create table
columns = dateColumn + varcharColumns + realColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
sql = 'ALTER TABLE "{}" ALTER COLUMN "年月日" TYPE date USING "年月日"::date;'.format(tablename)
cur.execute(sql)
conn.commit()
