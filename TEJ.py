
import sys, os
sys.path.append('/home/david/Dropbox/program/mypackage_py')
import psycopg2
import sqlCommand as sqlCommand
print(os.getcwd())
  # ----import----
from sqlite3 import *
import os
import time
import functools
from datetime import datetime, timedelta
from copy import deepcopy

import requests
from bs4 import BeautifulSoup
import numpy as np
from pandas import *
from functools import *

get_option("display.max_rows")
get_option("display.max_columns")
set_option("display.max_rows", 100)
set_option("display.max_columns", 1000)
set_option('display.expand_frame_repr', False)
set_option('display.unicode.east_asian_width', True)

## --- read from sqlite ---

os.chdir('/home/david/Documents/db/')

#--- summary ---

# --ac--
# connect
conn = psycopg2.connect("host=localhost dbname=tej user=postgres password=d03724008")
connLite = connect('TEJ.sqlite3')
cur = conn.cursor()
curLite = connLite.cursor()

# read from sqlite
sql = "SELECT * FROM '{}'"
tablename = 'tse_ch'
ac = read_sql_query(sql.format(tablename), connLite).replace('--', np.nan)
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
sqlCommand.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn)

# inset data
sqlCommand.insertDataPostgre(tablename, ac[columns], conn)