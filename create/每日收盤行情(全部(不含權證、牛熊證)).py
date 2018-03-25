import pandas as pd
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


#--- tse ---

# --close--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '每日收盤行情(全部(不含權證、牛熊證))'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

closeDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
closeDistictDateList = closeDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" where "年月日"="{}"'
close = pd.read_sql_query(sql.format(tablename, closeDistictDateList[0]), conn_lite)
close['漲跌(+/-)'] = close['漲跌(+/-)'].replace('＋', 1).replace('－', -1).replace('X', 0).replace(' ', 0).astype(float)
close['本益比'] = close['本益比'].replace('NaN', 0).fillna(0)

close.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
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
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
sql = 'ALTER TABLE "{}" ALTER COLUMN "年月日" TYPE date USING "年月日"::date;'.format(tablename)
cur.execute(sql)
conn_pg.commit()
