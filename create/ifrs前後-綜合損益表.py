import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc

# --- report ---

# --inc--
# connect
conn_lite = conn_local_lite('mops.sqlite3')
conn_pg = conn_local_pg('mops')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# cur.execute('SELECT * FROM "ifrs前後-綜合損益表"')
# conn_pg.commit()

# read from sqlite
sql = "SELECT * FROM '{}'"
tablename = 'ifrs前後-綜合損益表'
inc = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan)
inc.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
inc[varcharColumns] = inc[varcharColumns].astype(str)
inc[realColumns] = inc[realColumns].astype(float)

# create table
columns = varcharColumns + realColumns
fieldTypes = ['varchar(8)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年', '季', '公司代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
