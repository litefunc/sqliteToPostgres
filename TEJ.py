import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


# connect
conn_lite = conn_local_lite('TEJ.sqlite3')
conn_pg = conn_local_pg('tej')

# read from sqlite
sql = "SELECT * FROM '{}'"
tablename = 'tse_ch'
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['公司代號', '公司名稱', '產業別']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
ac[varcharColumns] = ac[varcharColumns].astype(str)
ac[realColumns] = ac[realColumns].astype(float)

# create table
columns = varcharColumns + realColumns
fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['公司代號', '公司名稱']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# inset data
sqlc.insertDataPostgre(tablename, ac[columns], conn_pg)