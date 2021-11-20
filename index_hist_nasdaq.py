import os
import sys
import json
import requests
from datetime import datetime, timedelta
import time
import cx_Oracle
from textwrap import dedent
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

### Common variable, params can be modified only here for change INDEX:
tmpTbl = "tmp_stock_index"
tgtTbl = "stock_index"
indexName = "NASDAQ"
indexSymbol = "^IXIC"
indexDesc = "NASDAQ Composite"
indexRegion = "US"
indexUrl = "https://yh-finance.p.rapidapi.com/stock/v3/get-historical-data"
indexHost = "yh-finance.p.rapidapi.com"
indexKey = "f6fd33cb75mshb502ad627cc30b5p142de5jsn241401eb5df7"

def cur_time():
    return datetime.today().strftime('%Y-%m-%d %H%M%S')
    
def callAPI(): 
    url = indexUrl
    headers = {
        'x-rapidapi-host': indexHost,
        'x-rapidapi-key': indexKey
    }
    queryString = {
        'symbol': indexSymbol,
        'region': indexRegion
    }
    histIndex = requests.request("GET", url, headers = headers, params = queryString)
    return histIndex
    
def insertToTmp(conn, curs, histIndexData, symbolCode, symbolName, symbolDesc):
    ### prepare data:
    insertData = []
    for i in histIndexData["prices"]:
        trxn_vol = i["volume"]
        close_price =  i["close"]
        open_price = i["open"]
        high_price = i["high"]
        low_price = i["low"]
        regular_market_time = i["date"]
        regular_market_date = datetime.utcfromtimestamp(regular_market_time)
        biz_dt_code = int(regular_market_date.strftime('%Y%m%d'))
        biz_date = regular_market_date.replace(hour=0, minute=0, second=0, microsecond = 0)
        biz_date_time = regular_market_date
        rowData = (symbolCode, symbolName, symbolDesc  ,"INDEX", "", trxn_vol, close_price, open_price, high_price, low_price, biz_dt_code, biz_date, biz_date_time, regular_market_time)
        insertData.append(rowData)
    ### TRUNCATE TMP TABLE
    truncateSql = 'TRUNCATE TABLE TMP_STOCK_INDEX'
    curs.execute(truncateSql)
    conn.commit()
    ### insert data to tmp
    insertSql = 'insert into TMP_STOCK_INDEX  (symbol_code, name, description, indicator_type, base_currency_code, trxn_vol, close_price, open_price, high_price, low_price, biz_dt_code, biz_date, biz_date_time, regular_market_time) values (:symbol_code, :name, :description, :indicator_type, :base_currency_code, :trxn_vol, :close_price, :open_price, :high_price, :low_price, :biz_dt_code, :biz_date, :biz_date_time,  :regular_market_time)'
    curs.executemany(insertSql, insertData)
    conn.commit()
    
def mergeToTgt(conn, curs):
    sql = 'MERGE INTO  STOCK_INDEX tgt '
    sql += ' USING TMP_STOCK_INDEX src '
    sql += ' on ( src.symbol_code = tgt.symbol_code AND src.biz_dt_code = tgt.biz_dt_code ) '
    sql += ' when not matched then '
    sql += ' insert (symbol_code, name, description, indicator_type, base_currency_code, trxn_vol, close_price, open_price, high_price, low_price, biz_dt_code, biz_date, biz_date_time, created_date, updated_date, regular_market_time) '
    sql += ' values (src.symbol_code ,src.name ,src.description ,src.indicator_type ,src.base_currency_code ,src.trxn_vol ,src.close_price ,src.open_price ,src.high_price ,src.low_price ,src.biz_dt_code ,src.biz_date ,src.biz_date_time ,src.created_date ,src.updated_date ,src.regular_market_time) '
    curs.execute(sql)
    conn.commit()

    
def executeIndexApi():
    histIndex = callAPI() #(indexUrl, indexHost, indexKey, indexSymbol, indexRegion)
    if histIndex.status_code == 200:
        ### create DB connection
        connDe = cx_Oracle.connect(user="de_prj", password="de_prj", dsn="192.168.21.132/orcl.localdomain")
        cursDe = connDe.cursor()
        ### insert data
        curData = histIndex.json()
        insertToTmp(connDe, cursDe, curData, indexSymbol, indexName, indexDesc)
        mergeToTgt(connDe, cursDe)
        connDe.close()
        
### Declare Dag
idxHistNasdaqDag = DAG('Index_Hist_NASDAQ', description='Index Stock Historical Data - NASDAQ Composite', schedule_interval= None  ,start_date =datetime(2021,11,15), catchup=False)

dagFunc_execute = PythonOperator(task_id = 'Execution', python_callable=executeIndexApi, dag=idxHistNasdaqDag)

dagFunc_execute