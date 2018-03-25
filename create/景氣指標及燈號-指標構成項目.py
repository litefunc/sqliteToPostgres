import pandas as pd
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


#---- bic ----

# --bic--
# connect
conn_lite = conn_local_lite('bic.sqlite3')
conn_pg = conn_local_pg('bic')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '景氣指標及燈號-指標構成項目'
sql="SELECT DISTINCT `年月` FROM '{}'".format(tablename)

bicDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
bicDistictDateList = bicDistictDate['年月'].tolist()

sql="SELECT * FROM '{}' WHERE 年月 = '{}'"
bic = pd.read_sql_query(sql.format(tablename, bicDistictDateList[0]), conn_lite).rename(columns={'工業及服務業受僱員工淨進入率(%)': '工業及服務業受僱員工淨進入率','失業率(%)': '失業率', '製造業存貨率(%)': '製造業存貨率'}).replace('--', 0).replace('NaN', 0).fillna(0)

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
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
