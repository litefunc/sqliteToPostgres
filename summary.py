
import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
syspath.append_if_not_exist('/home/david/program/python/project/crawler/finance/sqliteToPostgres')

import psycopg2
import sqlCommand as sqlc
import astype as ast
import dftosql
import sqlite3
import os
import pandas as pd
import numpy as np

pd.get_option("display.max_rows")
pd.get_option("display.max_columns")
pd.set_option("display.max_rows", 100)
pd.set_option("display.max_columns", 1000)
pd.set_option('display.expand_frame_repr', False)
pd.set_option('display.unicode.east_asian_width', True)

mode = 'create'
## --- read from sqlite ---

os.chdir('/home/david/Documents/db/')

#--- summary ---

# --ac--
# connect
conn = psycopg2.connect("host=localhost dbname=summary user=postgres password=d03724008")
connLite = sqlite3.connect('summary.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
sql = "SELECT * FROM '{}'"
tablename = '會計師查核報告'
ac = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan)
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
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn)

# insert data
dftosql.i_pg(conn, tablename, ac[cols])

# --ifrs前後-綜合損益表-一般業--
# connect
conn = psycopg2.connect("host=localhost dbname=summary user=postgres password=d03724008")
connLite = sqlite3.connect('summary.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
tablename = 'ifrs前後-綜合損益表-一般業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
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
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn)

# insert data
dftosql.i_pg(conn, tablename, ac[cols])

# --ifrs前後-綜合損益表-保險業--
# connect
conn = psycopg2.connect("host=localhost dbname=summary user=postgres password=d03724008")
connLite = sqlite3.connect('summary.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
tablename = 'ifrs前後-綜合損益表-保險業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
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
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn)

# insert data
dftosql.i_pg(conn, tablename, ac[cols])

# --ifrs前後-綜合損益表-其他業--
# connect
conn = psycopg2.connect("host=localhost dbname=summary user=postgres password=d03724008")
connLite = sqlite3.connect('summary.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
tablename = 'ifrs前後-綜合損益表-其他業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
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
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn)

# insert data
dftosql.i_pg(conn, tablename, ac[cols])

# --ifrs前後-綜合損益表-證券業--
# connect
conn = psycopg2.connect("host=localhost dbname=summary user=postgres password=d03724008")
connLite = sqlite3.connect('summary.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
tablename = 'ifrs前後-綜合損益表-證券業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
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
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn)

# insert data
dftosql.i_pg(conn, tablename, ac[cols])

# --ifrs前後-綜合損益表-金控業--
# connect
conn = psycopg2.connect("host=localhost dbname=summary user=postgres password=d03724008")
connLite = sqlite3.connect('summary.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
tablename = 'ifrs前後-綜合損益表-金控業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
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
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn)

# insert data
dftosql.i_pg(conn, tablename, ac[cols])

# --ifrs前後-綜合損益表-銀行業--
# connect
conn = psycopg2.connect("host=localhost dbname=summary user=postgres password=d03724008")
connLite = sqlite3.connect('summary.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
tablename = 'ifrs前後-綜合損益表-銀行業'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan).replace('None', np.nan).fillna(np.nan)
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
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn)

# insert data
dftosql.i_pg(conn, tablename, ac[cols])

# --營益分析--
# connect
conn = psycopg2.connect("host=localhost dbname=summary user=postgres password=d03724008")
connLite = sqlite3.connect('summary.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
tablename = '營益分析'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan).replace('\xa0', np.nan).replace('None', np.nan).fillna(np.nan)
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
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn)

# insert data
dftosql.i_pg(conn, tablename, ac[cols])

# --財務分析--
# connect
conn = psycopg2.connect("host=localhost dbname=summary user=postgres password=d03724008")
connLite = sqlite3.connect('summary.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
tablename = '財務分析'
sql = "SELECT * FROM '{}'"
ac = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan).replace('NA', np.nan).replace('None', np.nan).replace('*****', np.nan).fillna(np.nan)
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
    sqlc.createTablePostgre(tablename, cols, fieldTypes, primaryKeys, conn)

# insert data
dftosql.i_pg(conn, tablename, ac[cols])
