import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# --bal--
# connect
conn_lite = conn_local_lite('mops.sqlite3')
conn_pg = conn_local_pg('mops')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite
sql = "SELECT * FROM '{}'"
tablename = 'ifrs前後-資產負債表-一般業'
bal = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan)
bal.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
bal[varcharColumns] = bal[varcharColumns].astype(str)
bal[realColumns] = bal[realColumns].astype(float)

# create table
columns = varcharColumns + realColumns
fieldTypes = ['varchar(8)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年', '季', '公司代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
