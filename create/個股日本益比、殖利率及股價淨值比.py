import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --value--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '個股日本益比、殖利率及股價淨值比'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

valueDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
valueDistictDateList = valueDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
value = pd.read_sql_query(sql.format(tablename, valueDistictDateList[0]), conn_lite).replace('--', np.nan) # column name can not contain % otherwise data can't be inserted
value['本益比'] = value['本益比'].replace('-', 0).replace('NaN', 0).fillna(0)
value['股價淨值比'] = value['股價淨值比'].replace('-', 0).replace('NaN', 0).fillna(0)

value.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
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
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
