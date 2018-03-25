import pandas as pd
import numpy as np
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc
import astype as ast
import dftosql

mode = 'create'
## --- read from sqlite ---

os.chdir('/home/david/Documents/db/')

#--- summary ---
# connect
conn_lite = conn_local_lite('summary.sqlite3')
conn_pg = conn_local_pg('summary')

# --ac--

# read from sqlite
sql = "SELECT * FROM '{}'"
tablename = '會計師查核報告'
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['年', '季', '公司代號', '公司簡稱', '簽證會計師事務所名稱', '簽證會計師', '簽證會計師.1', '核閱或查核日期', '核閱或查核報告類型']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
ac[varcharColumns] = ac[varcharColumns].astype(str)
ac[realColumns] = ac[realColumns].astype(float)

cols = varcharColumns + realColumns

# create table
if mode == 'create':
    fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
    primaryKeys = ['年', '季', '公司代號']
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn_pg)

# insert data
dftosql.i_pg(conn_pg, tablename, ac[cols])

# --ifrs前後-綜合損益表-一般業--
# connect

# read from sqlite
tablename = 'ifrs前後-綜合損益表-一般業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
ac[varcharColumns] = ac[varcharColumns].astype(str)
ac[realColumns] = ac[realColumns].astype(float)

cols = varcharColumns + realColumns

# create table
if mode == 'create':
    fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
    primaryKeys = ['年', '季', '公司代號']
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn_pg)

# insert data
dftosql.i_pg(conn_pg, tablename, ac[cols])

# --ifrs前後-綜合損益表-保險業--

# read from sqlite
tablename = 'ifrs前後-綜合損益表-保險業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
ac[varcharColumns] = ac[varcharColumns].astype(str)
ac[realColumns] = ac[realColumns].astype(float)

cols = varcharColumns + realColumns

# create table
if mode == 'create':
    fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
    primaryKeys = ['年', '季', '公司代號']
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn_pg)

# insert data
dftosql.i_pg(conn_pg, tablename, ac[cols])

# --ifrs前後-綜合損益表-其他業--

# read from sqlite
tablename = 'ifrs前後-綜合損益表-其他業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
ac[varcharColumns] = ac[varcharColumns].astype(str)
ac[realColumns] = ac[realColumns].astype(float)

cols = varcharColumns + realColumns

# create table
if mode == 'create':
    fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
    primaryKeys = ['年', '季', '公司代號']
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn_pg)

# insert data
dftosql.i_pg(conn_pg, tablename, ac[cols])

# --ifrs前後-綜合損益表-證券業--

# read from sqlite
tablename = 'ifrs前後-綜合損益表-證券業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
ac[varcharColumns] = ac[varcharColumns].astype(str)
ac[realColumns] = ac[realColumns].astype(float)

cols = varcharColumns + realColumns

# create table
if mode == 'create':
    fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
    primaryKeys = ['年', '季', '公司代號']
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn_pg)

# insert data
dftosql.i_pg(conn_pg, tablename, ac[cols])

# --ifrs前後-綜合損益表-金控業--

# read from sqlite
tablename = 'ifrs前後-綜合損益表-金控業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
ac[varcharColumns] = ac[varcharColumns].astype(str)
ac[realColumns] = ac[realColumns].astype(float)

cols = varcharColumns + realColumns

# create table
if mode == 'create':
    fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
    primaryKeys = ['年', '季', '公司代號']
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn_pg)

# insert data
dftosql.i_pg(conn_pg, tablename, ac[cols])

# --ifrs前後-綜合損益表-銀行業--

# read from sqlite
tablename = 'ifrs前後-綜合損益表-銀行業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
ac[varcharColumns] = ac[varcharColumns].astype(str)
ac[realColumns] = ac[realColumns].astype(float)

cols = varcharColumns + realColumns

# create table
if mode == 'create':
    fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
    primaryKeys = ['年', '季', '公司代號']
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn_pg)

# insert data
dftosql.i_pg(conn_pg, tablename, ac[cols])

# --營益分析--

# read from sqlite
tablename = '營益分析'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan).replace('\xa0', np.nan).replace('None', np.nan).fillna(np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
dtypes = {'str':varcharColumns, 'float':realColumns}
ac = ast.as_type(dtypes, ac)

cols = varcharColumns + realColumns

# create table
if mode == 'create':
    fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
    primaryKeys = ['年', '季', '公司代號']
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn_pg)

# insert data
dftosql.i_pg(conn_pg, tablename, ac[cols])

# --財務分析--

# read from sqlite
tablename = '財務分析'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan).replace('NA', np.nan).replace('None', np.nan).replace('*****', np.nan).fillna(np.nan)
ac.dtypes
columns = list(ac)
varcharColumns = ['年', '公司代號', '公司簡稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
dtypes = {'str':varcharColumns, 'float':realColumns}
ast.to_float(realColumns, ac)
ac = ast.as_type(dtypes, ac)

cols = varcharColumns + realColumns

# create table
if mode == 'create':
    fieldTypes = ['varchar' for col in varcharColumns] + ['real' for col in realColumns]
    primaryKeys = ['年', '公司代號', '公司簡稱']
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn_pg)

# insert data
dftosql.i_pg(conn_pg, tablename, ac[cols])
