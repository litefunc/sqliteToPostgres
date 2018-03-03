import sys, os
sys.path.append(os.getenv('MY_PYTHON_PKG'))
import syspath

import sqlCommand as sqlc
import pandas as pd
import psycopg2
import sqlite3

syspath.append_if_not_exist('/home/david/program/python/project/crawler/finance/sqliteToPostgres')
import create

## --- read from sqlite ---

os.chdir(create.dbpath)

# --- report ---

# --inc--
# connect
conn = psycopg2.connect("host=localhost dbname=mops user=postgres password=d03724008")
connLite = sqlite3.connect('mops.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# cur.execute('SELECT * FROM "ifrs前後-綜合損益表"')
# conn.commit()

# read from sqlite
sql = "SELECT * FROM '{}'"
tablename = 'ifrs前後-綜合損益表'
inc = pd.read_sql_query(sql.format(tablename), connLite).replace('--', np.nan)
inc.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), connLite))
varcharColumns = ['年', '季', '公司代號', '公司名稱']
realColumns = list(filter(lambda x: x not in varcharColumns, columns))
inc[varcharColumns] = inc[varcharColumns].astype(str)
inc[realColumns] = inc[realColumns].astype(float)

# create table
columns = varcharColumns + realColumns
fieldTypes = ['varchar(8)' for col in varcharColumns] + ['real' for col in realColumns]
primaryKeys = ['年', '季', '公司代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)
