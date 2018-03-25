import numpy as np
import pandas as pd
import os
import sys

if os.getenv('MY_PYTHON_PKG') not in sys.path:
    sys.path.append(os.getenv('MY_PYTHON_PKG'))

import syspath
from common.connection import conn_local_lite, conn_local_pg
import sqlCommand as sqlc


def join(s, l):
    return s.join(l)

def tostring(df, cols):
    l = list(df)
    for col in cols:
        if col in l:
            df[[col]] = df[[col]].astype(str)
    return df

def toDate(df, cols):
    l = list(df)
    for col in cols:
        if col in l:
            df[col] = pd.to_datetime(df[col]).apply(lambda x: x.date())
    return df

def identity(x):
    return x

def update(tablename, cols, date_columns=[], varchar_columns=[], real_columns=[], transform=identity):
    sql1 = 'SELECT distinct {} FROM "{}"'.format(sqlc.quoteToString("`", cols), tablename)
    df1 = toDate(pd.read_sql_query(sql1, conn_lite), date_columns)  # in order to convert %y/%m/%d to %y-%m-%d
    df1 = tostring(df1, varchar_columns)
    rows1 = tostring(df1, date_columns).to_dict('records')

    sql2 = 'SELECT distinct {} FROM "{}"'.format(sqlc.quoteToString('"', cols), tablename)
    df2 = toDate(pd.read_sql_query(sql2, conn_pg), date_columns)
    df2 = tostring(df2, varchar_columns)
    rows2 = tostring(df2, date_columns).to_dict('records')

    rows = [key for key in rows1 if key not in rows2]

    for row in rows:
        sql = 'SELECT * FROM "{}" where {}'.format(tablename, join(' and ', ['"{0}"="{1}"'.format(key, value) for key, value in row.items()]))
        df = pd.read_sql_query(sql, conn_lite)
        sqlc.insertDataPostgre(tablename, transform(date_columns, varchar_columns, real_columns, df), conn_pg)


## --- read from sqlite ---

# --- report ---

# --inc--
# connect
conn_lite = conn_local_lite('mops.sqlite3')
conn_pg = conn_local_pg('mops')
cur_pg = conn_pg.cursor()
cur_lite = conn_lite.cursor()

# cur.execute('SELECT * FROM "ifrs前後-綜合損益表"')
# conn_pg.commit()

# read from sqlite
sql = "SELECT * FROM '{}'"
tablename = 'ifrs前後-綜合損益表'
inc = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan)
inc.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
varchar_columns = ['年', '季', '公司代號', '公司名稱']
real_columns = list(filter(lambda x: x not in varchar_columns, columns))
inc[varchar_columns] = inc[varchar_columns].astype(str)
inc[real_columns] = inc[real_columns].astype(float)

# create table
columns = varchar_columns + real_columns
fieldTypes = ['varchar(8)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年', '季', '公司代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# inset data
# sqlc.insertDataPostgre(tablename, inc[columns], conn_pg)

def transform(date_columns=[], varchar_columns=[], real_columns=[], df=pd.DataFrame()):
    df = df.replace('--', np.nan)
    return df

# update
update(tablename, ['年', '季'], [], varchar_columns, real_columns, transform)

# drop table
# sqlc.dropTablePostgre(tablename, conn_pg)

# --bal--
# read from sqlite
tablename = 'ifrs前後-資產負債表-一般業'
bal = pd.read_sql_query(sql.format(tablename), conn_lite).replace('--', np.nan)
bal.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
varchar_columns = ['年', '季', '公司代號', '公司名稱']
real_columns = list(filter(lambda x: x not in varchar_columns, columns))
bal[varchar_columns] = bal[varchar_columns].astype(str)
bal[real_columns] = bal[real_columns].astype(float)

# create table
columns = varchar_columns + real_columns
fieldTypes = ['varchar(8)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年', '季', '公司代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# inset data
# sqlc.insertDataPostgre(tablename, bal[columns], conn_pg)

# update
update(tablename, ['年', '季'], [], varchar_columns, real_columns, transform)

#--- tse ---

# --close--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '每日收盤行情(全部(不含權證、牛熊證))'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

closeDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
closeDistictDateList = closeDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" where "年月日"="{}"'
close = pd.read_sql_query(sql.format(tablename, closeDistictDateList[0]), conn_lite)
close['漲跌(+/-)'] = close['漲跌(+/-)'].replace('＋', 1).replace('－', -1).replace('X', 0).replace(' ', 0).astype(float)
close['本益比'] = close['本益比'].replace('NaN', 0).fillna(0)

close.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = ['年月日']
varchar_columns = ['證券代號', '證券名稱']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
close.年月日 = pd.to_datetime(close.年月日).apply(lambda x: x.date()).astype(str)

close[varchar_columns] = close[varchar_columns].astype(str)
close[real_columns] = close[real_columns].astype(float)

# create table
columns = date_columns + varchar_columns + real_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)
sql = 'ALTER TABLE "{}" ALTER COLUMN "年月日" TYPE date USING "年月日"::date;'.format(tablename)
cur.execute(sql)
conn_pg.commit()


def transform(date_columns=[], varchar_columns=[], real_columns=[], close=pd.DataFrame()):
    close = close.replace('--', np.nan)
    close['漲跌(+/-)'] = close['漲跌(+/-)'].replace('＋', 1).replace('－', -1).replace('X', 0).replace(' ', 0)
    close['本益比'] = close['本益比'].replace('NaN', 0).fillna(0)  # pe is '0.00' when pe < 0
    close.年月日 = pd.to_datetime(close.年月日).apply(lambda x: x.date()).astype(str)
    close[varchar_columns] = close[varchar_columns].astype(str)
    close[real_columns] = close[real_columns].astype(float)
    return close

# update
update(tablename, ['年月日'], date_columns, varchar_columns, real_columns, transform)


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
date_columns = ['年月日']
varchar_columns = ['證券代號', '證券名稱', '財報年/季']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
value.年月日 = pd.to_datetime(value.年月日).apply(lambda x: x.date()).astype(str)
value[varchar_columns] = value[varchar_columns].astype(str)
value[real_columns] = value[real_columns].astype(float)

# create table
columns = date_columns + varchar_columns + real_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

def transform(date_columns=[], varchar_columns=[], real_columns=[], df=pd.DataFrame()):
    df['本益比'] = df['本益比'].replace('0.00', 0).replace('-', 0).replace('NaN', 0).fillna(0)
    df['股價淨值比'] = df['股價淨值比'].replace('0.00', 0).replace('-', 0).replace('NaN', 0).fillna(0)
    df = df.replace('--', np.nan)
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[real_columns] = df[real_columns].astype(float)
    return df

# sql = 'ALTER TABLE "{}" ALTER COLUMN "財報年/季" TYPE varchar(14) USING "財報年/季"::varchar(14);'.format(tablename)
# cur.execute(sql)
# conn_pg.commit()
#
# cur.execute('delete from "個股日本益比、殖利率及股價淨值比" where "財報年/季" IS NULL;')
# conn_pg.commit()
# 'select * from "個股日本益比、殖利率及股價淨值比" where "財報年/季" IS NULL;'
# df = pd.read_sql_query("select * from '{}' where '{}'='{}'".format(tablename,'財報年/季','NULL'), conn_pg)
# df = pd.read_sql_query('select * from "{}" where "{}"="{}"'.format(tablename,'財報年/季','2017/3'), conn_lite)
# df[real_columns] = df[real_columns].astype(float)
# conn_lite.commit()

# update
update(tablename, ['年月日'], date_columns, varchar_columns, real_columns, transform)


# --margin--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '當日融券賣出與借券賣出成交量值(元)'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

marginDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
marginDistictDateList = marginDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
margin = pd.read_sql_query(sql.format(tablename, marginDistictDateList[0]), conn_lite).replace('--', np.nan)

margin.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = ['年月日']
varchar_columns = ['證券代號', '證券名稱']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
margin.年月日 = pd.to_datetime(margin.年月日).apply(lambda x: x.date()).astype(str)
margin[varchar_columns] = margin[varchar_columns].astype(str)
margin[real_columns] = margin[real_columns].astype(float)

# create table
columns = date_columns + varchar_columns + real_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update
def transform(date_columns=[], varchar_columns=[], real_columns=[], df=pd.DataFrame()):
    df = df.replace('--', np.nan)
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[real_columns] = df[real_columns].astype(float)
    return df

update(tablename, ['年月日'], date_columns, varchar_columns, real_columns, transform)

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
date_columns = ['年月日']
varchar_columns = ['證券代號', '證券名稱']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
ins.年月日 = pd.to_datetime(ins.年月日).apply(lambda x: x.date()).astype(str)
ins[varchar_columns] = ins[varchar_columns].astype(str)
ins[real_columns] = ins[real_columns].astype(float)

# create table
columns = date_columns + varchar_columns + real_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update
def transform(date_columns=[], varchar_columns=[], real_columns=[], df=pd.DataFrame()):
    df = df.replace('--', np.nan).replace('NaN', 0).fillna(0)
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[real_columns] = df[real_columns].astype(float)
    return df

update(tablename, ['年月日'], date_columns, varchar_columns, real_columns, transform)

# --deal--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '自營商買賣超彙總表 (股)'
sql="SELECT DISTINCT `年月日` FROM `{}`".format(tablename)

dealDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
dealDistictDateList = dealDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
deal = pd.read_sql_query(sql.format(tablename, dealDistictDateList[0]), conn_lite).replace('--', np.nan).replace('NaN', 0).fillna(0)
deal[['自營商(自行買賣)賣出股數', '自營商(自行買賣)買賣超股數', '自營商(自行買賣)買進股數', '自營商(避險)賣出股數', '自營商(避險)買賣超股數', '自營商(避險)買進股數', '自營商賣出股數', '自營商買賣超股數', '自營商買進股數']] = deal[['自營商(自行買賣)賣出股數', '自營商(自行買賣)買賣超股數', '自營商(自行買賣)買進股數', '自營商(避險)賣出股數', '自營商(避險)買賣超股數', '自營商(避險)買進股數', '自營商賣出股數', '自營商買賣超股數', '自營商買進股數']].fillna(0)

deal.dtypes
columns = list(pd.read_sql_query("SELECT * FROM `{}` limit 1".format(tablename), conn_lite))
date_columns = ['年月日']
varchar_columns = ['證券代號', '證券名稱']
integer_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
deal.年月日 = pd.to_datetime(deal.年月日).apply(lambda x: x.date()).astype(str)
deal[varchar_columns] = deal[varchar_columns].astype(str)
deal[integer_columns] = deal[integer_columns].astype(int)

# create table
columns = date_columns + varchar_columns + integer_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['integer' for col in integer_columns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update
def transform(date_columns=[], varchar_columns=[], integer_columns=[], df=pd.DataFrame()):
    df = df.replace('--', np.nan).replace('NaN', 0).fillna(0)
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[integer_columns] = df[integer_columns].astype(int)
    return df

update(tablename, ['年月日'], date_columns, varchar_columns, integer_columns, transform)

# --fore--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '外資及陸資買賣超彙總表 (股)'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

foreDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
foreDistictDateList = foreDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
fore = pd.read_sql_query(sql.format(tablename, foreDistictDateList[0]), conn_lite).rename(columns={'買進股數':'外資買進股數','賣出股數':'外資賣出股數','買賣超股數':'外資買賣超股數','鉅額交易': '外資鉅額交易'}).replace('--', np.nan).replace('NaN', 0).fillna(0)
fore[['外資鉅額交易']] = fore[['外資鉅額交易']].applymap(lambda x:0 if x == ' ' else 1)

set(fore.外資鉅額交易)
fore.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = ['年月日']
varchar_columns = ['證券代號', '證券名稱']
integer_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
fore.年月日 = pd.to_datetime(fore.年月日).apply(lambda x: x.date()).astype(str)
fore[varchar_columns] = fore[varchar_columns].astype(str)
fore[integer_columns] = fore[integer_columns].astype(int)

# create table
columns = date_columns + varchar_columns + integer_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['integer' for col in integer_columns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update

def transform(date_columns=[], varchar_columns=[], integer_columns=[], df=pd.DataFrame()):
    df = df.rename(columns={'買進股數': '外資買進股數', '賣出股數': '外資賣出股數', '買賣超股數': '外資買賣超股數', '鉅額交易': '外資鉅額交易'}).replace('--',np.nan).replace('NaN', 0).fillna(0)
    df[['外資鉅額交易']] = df[['外資鉅額交易']].applymap(lambda x: 0 if x == ' ' else 1)
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[integer_columns] = df[integer_columns].astype(int)
    return df

update(tablename, ['年月日'], date_columns, varchar_columns, integer_columns, transform)

# --trust--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '投信買賣超彙總表 (股)'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

trustDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
trustDistictDateList = trustDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
trust = pd.read_sql_query(sql.format(tablename, trustDistictDateList[0]), conn_lite).rename(columns={'買進股數':'投信買進股數','賣出股數':'投信賣出股數','買賣超股數':'投信買賣超股數','鉅額交易': '投信鉅額交易'}).replace('--', np.nan).replace('NaN', 0).fillna(0)
trust[['投信鉅額交易']] = trust[['投信鉅額交易']].applymap(lambda x:0 if x == ' ' else 1)

trust.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = ['年月日']
varchar_columns = ['證券代號', '證券名稱']
integer_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
trust.年月日 = pd.to_datetime(trust.年月日).apply(lambda x: x.date()).astype(str)
trust[varchar_columns] = trust[varchar_columns].astype(str)
trust[integer_columns] = trust[integer_columns].astype(int)

# create table
columns = date_columns + varchar_columns + integer_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['integer' for col in integer_columns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update

def transform(date_columns=[], varchar_columns=[], integer_columns=[], df=pd.DataFrame()):
    df = df.rename(
        columns={'買進股數': '投信買進股數', '賣出股數': '投信賣出股數', '買賣超股數': '投信買賣超股數', '鉅額交易': '投信鉅額交易'})
    df[['投信買進股數', '投信賣出股數', '投信買賣超股數']] = df[['投信買進股數', '投信賣出股數', '投信買賣超股數']].replace('--', np.nan).replace('NaN', 0).fillna(0)
    df[['投信鉅額交易']] = df[['投信鉅額交易']].applymap(lambda x: 0 if x == ' ' else 1)
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[integer_columns] = df[integer_columns].astype(int)
    return df


update(tablename, ['年月日'], date_columns, varchar_columns, integer_columns, transform)

# --index--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = 'index'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

indexDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
indexDistictDateList = indexDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
index = pd.read_sql_query(sql.format(tablename, indexDistictDateList[0]), conn_lite).replace('--', 0).replace('---', 0).replace('NaN', 0).fillna(0)
index.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = ['年月日']
varchar_columns = []
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
index.年月日 = pd.to_datetime(index.年月日).apply(lambda x: x.date()).astype(str)
index[varchar_columns] = index[varchar_columns].astype(str)
index[real_columns] = index[real_columns].astype(float)

# create table
columns = date_columns + varchar_columns + real_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月日']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update

def transform(date_columns=[], varchar_columns=[], integer_columns=[], df=pd.DataFrame()):
    df = df.replace('--', 0).replace('---', 0).replace('NaN', 0).fillna(0)
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[real_columns] = df[real_columns].astype(float)
    return df

update(tablename, ['年月日'], date_columns, varchar_columns, integer_columns, transform)

# --xdr--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '除權息計算結果表'
sql="SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

xdrDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
xdrDistictDateList = xdrDistictDate['年月日'].tolist()

sql='SELECT * FROM "{}" WHERE "年月日"="{}"'
xdr = pd.read_sql_query(sql.format(tablename, xdrDistictDateList[0]), conn_lite).rename(columns={'股票代號': '證券代號', '股票名稱': '證券名稱'})

xdr.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = ['年月日']
varchar_columns = ['證券代號', '證券名稱']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
xdr.年月日 = pd.to_datetime(xdr.年月日).apply(lambda x: x.date()).astype(str)
xdr[varchar_columns] = xdr[varchar_columns].astype(str)
xdr[real_columns] = xdr[real_columns].astype(float)

# create table
columns = date_columns + varchar_columns + real_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月日', '證券代號']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update

def transform(date_columns=[], varchar_columns=[], integer_columns=[], df=pd.DataFrame()):
    df = df.rename(columns={'股票代號': '證券代號', '股票名稱': '證券名稱'})
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[real_columns] = df[real_columns].astype(float)
    return df

update(tablename, ['年月日'], date_columns, varchar_columns, integer_columns, transform)

# --大盤成交統計--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '大盤成交統計'
sql = "SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

dfDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
dfDistictDateList = dfDistictDate['年月日'].tolist()

sql = 'SELECT * FROM "{}" WHERE "年月日"="{}"'
df = pd.read_sql_query(sql.format(tablename, dfDistictDateList[-1]), conn_lite).replace('--', 0).replace('NaN', 0).fillna(0)

df.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = ['年月日']
varchar_columns = ['成交統計']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
df[varchar_columns] = df[varchar_columns].astype(str)
df[real_columns] = df[real_columns].astype(float)

# create table
columns = date_columns + varchar_columns + real_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月日', '成交統計']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update

def transform(date_columns=[], varchar_columns=[], integer_columns=[], df=pd.DataFrame()):
    df = df.replace('--', 0).replace('NaN', 0).fillna(0)
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[real_columns] = df[real_columns].astype(float)
    return df

update(tablename, ['年月日'], date_columns, varchar_columns, integer_columns, transform)

# --大盤統計資訊--
# connect
conn_lite = conn_local_lite('tse.sqlite3')
conn_pg = conn_local_pg('tse')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '大盤統計資訊'
sql = "SELECT DISTINCT `年月日` FROM '{}'".format(tablename)

dfDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
dfDistictDateList = dfDistictDate['年月日'].tolist()

sql = 'SELECT * FROM "{}" WHERE "年月日"="{}"'
df = pd.read_sql_query(sql.format(tablename, dfDistictDateList[-1]), conn_lite).replace('--', 0).replace('NaN', 0).fillna(0)

df.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = ['年月日']
varchar_columns = ['指數']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)

df[varchar_columns] = df[varchar_columns].astype(str)
df[real_columns] = df[real_columns].astype(float)

# create table
columns = date_columns + varchar_columns + real_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月日', '指數']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update

def transform(date_columns=[], varchar_columns=[], integer_columns=[], df=pd.DataFrame()):
    df = df.replace('--', 0).replace('NaN', 0).fillna(0)
    df.年月日 = pd.to_datetime(df.年月日).apply(lambda x: x.date()).astype(str)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[real_columns] = df[real_columns].astype(float)
    return df

update(tablename, ['年月日'], date_columns, varchar_columns, integer_columns, transform)

#---- bic ----

# --bic--
# connect
conn_lite = conn_local_lite('bic.sqlite3')
conn_pg = conn_local_pg('bic')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '景氣指標及燈號-指標構成項目'
sql="SELECT DISTINCT `年月` FROM '{}'".format(tablename)

bicDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
bicDistictDateList = bicDistictDate['年月'].tolist()

sql="SELECT * FROM '{}' WHERE 年月 = '{}'"
bic = pd.read_sql_query(sql.format(tablename, bicDistictDateList[0]), conn_lite).rename(columns={'工業及服務業受僱員工淨進入率(%)': '工業及服務業受僱員工淨進入率','失業率(%)': '失業率', '製造業存貨率(%)': '製造業存貨率'}).replace('--', 0).replace('NaN', 0).fillna(0)

bic.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = []
varchar_columns = ['年月', '年', '月']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
bic[varchar_columns] = bic[varchar_columns].astype(str)
bic[real_columns] = bic[real_columns].astype(float)

# create table
columns = date_columns + varchar_columns + real_columns
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update

def transform(date_columns=[], varchar_columns=[], integer_columns=[], df=pd.DataFrame()):
    df = df.rename(columns={'工業及服務業受僱員工淨進入率(%)': '工業及服務業受僱員工淨進入率','失業率(%)': '失業率', '製造業存貨率(%)': '製造業存貨率'})[columns].replace('--', 0).replace('NaN', 0).fillna(0)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[real_columns] = df[real_columns].astype(float)
    return df


update(tablename, ['年月'], date_columns, varchar_columns, integer_columns, transform)

# --compositeIndex--
# connect
conn_lite = conn_local_lite('bic.sqlite3')
conn_pg = conn_local_pg('bic')
cur = conn_pg.cursor()
curLite = conn_lite.cursor()

# read from sqlite

tablename = '景氣指標及燈號-綜合指數'
sql="SELECT DISTINCT `年月` FROM '{}'".format(tablename)

compositeIndexDistictDate = pd.read_sql_query(sql.format(tablename), conn_lite)
compositeIndexDistictDateList = compositeIndexDistictDate['年月'].tolist()

sql="SELECT * FROM '{}' WHERE 年月 = '{}'"
compositeIndex = pd.read_sql_query(sql.format(tablename, compositeIndexDistictDateList[0]), conn_lite).replace('--', 0).replace('NaN', 0).fillna(0)

compositeIndex.dtypes
columns = list(pd.read_sql_query("SELECT * FROM '{}' limit 1".format(tablename), conn_lite))
date_columns = []
varchar_columns = ['年月', '年', '月']
real_columns = list(filter(lambda x: x not in (date_columns + varchar_columns), columns))
compositeIndex[varchar_columns] = compositeIndex[varchar_columns].astype(str)
compositeIndex[real_columns] = compositeIndex[real_columns].astype(float)

# create table
fieldTypes = ['date' for col in date_columns] + ['varchar(14)' for col in varchar_columns] + ['real' for col in real_columns]
primaryKeys = ['年月']
sqlc.createTablePostgre(tablename, columns, fieldTypes, primaryKeys, conn_pg)

# update
    
def transform(date_columns=[], varchar_columns=[], integer_columns=[], df=pd.DataFrame()):
    df = df.replace('--', 0).replace('NaN', 0).fillna(0)
    df[varchar_columns] = df[varchar_columns].astype(str)
    df[real_columns] = df[real_columns].astype(float)
    return df

update(tablename, ['年月'], date_columns, varchar_columns, integer_columns, transform)

