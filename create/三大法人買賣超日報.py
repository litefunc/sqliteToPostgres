import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --ins--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '三大法人買賣超日報'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

insDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
insDistictDateList = insDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
ins = pd.read_sql_query(sql.format(tablename, insDistictDateList[0]), conn_lite).replace('NaN', 0).fillna(0)

ins.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
dateColumn = ['年月日']
varcharColumns = ['證券代號', '證券名稱']
realColumns = list(filter(lambda x: x not in (dateColumn + varcharColumns), columns))
ins.年月日 = pd.to_datetime(ins.年月日).apply(lambda x: x.date()).astype(str)
ins[varcharColumns] = ins[varcharColumns].astype(str)
ins[realColumns] = ins[realColumns].astype(float)

# create table
columns = dateColumn + varcharColumns + realColumns
fieldTypes = ['date' for col in dateColumn] + ['varchar(14)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
