import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath
import sqlCommand as sqlc
import pandas as pd
import numpy as np
import psycopg2
import sqlite3

syspath.append_if_not_exist('/home/david/program/python/project/crawler/finance/sqliteToPostgres')
import create

## --- read from sqlite ---

os.chdir(create.dbpath)

# --bal--
# connect
conn = psycopg2.connect("host=localhost dbname=mops user=postgres password=d03724008")
connLite = sqlite3.connect('mops.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
sql = "SELECT * FROM '{}'"
tablename = 'ifrs前後-資產負債表-一般業'
bal = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan)
bal.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), connLite))
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
bal[varcharColumns] = bal[varcharColumns].astype(str)
bal[realColumns] = bal[realColumns].astype(float)

# create table
columns = varcharColumns + realColumns
fieldTypes = ['varchar(8)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年', '季', '公司代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
