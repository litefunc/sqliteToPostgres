import pandas as pd
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --compositeIndex--
# connect
conn_lite = conn_local_lite('bic.sqlite3')
conn_pg = conn_local_pg('bic')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '景氣指標及燈號-綜合指數'
sql="SELECT DISTINCT `年月` FROM '{}'".format(tablename)

compositeIndexDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
compositeIndexDistictDateList = compositeIndexDistictDate['年月'].tolist()

sql="SELECT * FROM '{}' WHERE 年月 = '{}'"
compositeIndex = pd.read_sql_query(sql.format(tablename, compositeIndexDistictDateList[0]), conn_lite).replace('--', 0).replace('NaN', 0).fillna(0)

compositeIndex.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
dateColumn = []
varcharColumns = ['年月', '年', '月']
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
compositeIndex[varcharColumns] = compositeIndex[varcharColumns].astype(str)
compositeIndex[realColumns] = compositeIndex[realColumns].astype(float)

# create table
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
