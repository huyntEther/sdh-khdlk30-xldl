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
logPath = "/opt/af-logs/index_sp500.txt"
tmpTbl = "tmp_stock_index"
tgtTbl = "stock_index"
indexName = "S&P 500"
indexSymbol = "^GSPC"
indexRegion = "US"
indexUrl = "https://yh-finance.p.rapidapi.com/stock/v2/get-summary"
indexHost = "yh-finance.p.rapidapi.com"
indexKey = "f6fd33cb75mshb502ad627cc30b5p142de5jsn241401eb5df7"

### global var
#status_code = ""
#biz_dt_code  = ""
#biz_date_time = ""
### Function
def cur_time():
    return datetime.today().strftime('%Y-%m-%d %H%M%S')

def write_log(file_path, input_str):
    #cur_time = cur_time()
    f = open(file_path, "a")
    f.write(input_str)
    f.close()

def write_log_init(**kwargs):
    file_path = kwargs["file_path"]
    # Write to log file execution time
    f = open(file_path, "a")
    callApiLog = "\nCall API for\t\t" +indexName +"\t\twith code " + indexSymbol
    f.write("\n-------------")
    f.write(callApiLog)
    curTime = "\nStart at " + cur_time()
    f.write(curTime)
    f.close()

def write_log_api_status(file_path, apiStatus):
    f = open(file_path, "a")
    f.write("\nStatus is " + str(apiStatus))
    f.close()
    
def callAPI(): #(indexUrl, indexHost, indexKey, indexSymbol, indexRegion):
    ### Init log of API call
    #write_log(logPath, "\n---------")
    #callApiLog = "\nCall API for\t\t" +indexName +"\t\twith code" + indexSymbol +"\n"
    #write_log(logPath, callApiLog)
    ### calApi
    url = indexUrl
    headers = {
        'x-rapidapi-host': indexHost,
        'x-rapidapi-key': indexKey
    }
    queryString = {
        'symbol': indexSymbol,
        'region': indexRegion
    }
    dailyIndex = requests.request("GET", url, headers = headers, params = queryString)
    return dailyIndex
    
def insertToTmp(conn, curs, dailyIndexData, symbolCode, symbolName):
    ### prepare data:
    shortName = dailyIndexData["quoteType"]["shortName"]
    trxn_vol = dailyIndexData["summaryDetail"]["volume"]["raw"]
    close_price =  dailyIndexData["summaryDetail"]["regularMarketPreviousClose"]["raw"]
    open_price = dailyIndexData["summaryDetail"]["regularMarketOpen"]["raw"]
    high_price = dailyIndexData["summaryDetail"]["regularMarketDayHigh"]["raw"]
    low_price = dailyIndexData["summaryDetail"]["regularMarketDayLow"]["raw"]
    regular_market_time = dailyIndexData["price"]["regularMarketTime"]
    regular_market_date = datetime.utcfromtimestamp(regular_market_time)
    biz_dt_code = int(regular_market_date.strftime('%Y%m%d'))
    biz_date = regular_market_date.replace(hour=0, minute=0, second=0, microsecond = 0)
    biz_date_time = regular_market_date
    ### TRUNCATE TMP TABLE
    truncateSql = 'TRUNCATE TABLE TMP_STOCK_INDEX'
    curs.execute(truncateSql)
    conn.commit()
    ### insert data to tmp
    insertSql = 'insert into TMP_STOCK_INDEX  (symbol_code, name, description, indicator_type, base_currency_code, trxn_vol, close_price, open_price, high_price, low_price, biz_dt_code, biz_date, biz_date_time, regular_market_time) values (:symbol_code, :name, :description, :indicator_type, :base_currency_code, :trxn_vol, :close_price, :open_price, :high_price, :low_price, :biz_dt_code, :biz_date, :biz_date_time,  :regular_market_time)'
    insertData = [symbolCode, symbolName, shortName  ,"INDEX", "", trxn_vol, close_price, open_price, high_price, low_price, biz_dt_code, biz_date, biz_date_time, regular_market_time ]
    curs.execute(insertSql, insertData)
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


def db_log(statusCode):
    ### prepare data
    status_code = statusCode
    logSql = 'insert into api_daily_log (status_code	, status_desc	, api_table	, api_type	, api_code	, biz_dt_code	,  biz_date_time) values (:status_code	, :status_desc, :api_table	, :api_type	, :api_code	, :biz_dt_code	,  :biz_date_time)'
    if status_code == 200:
        status_desc = "SUCCESS"
    else:
        status_desc = "FAIL"
    api_table = "STOCK_INDEX"
    api_type = "INDEX"
    api_code = indexSymbol
    curDate = datetime.utcnow()
    biz_dt_code = int(curDate.strftime('%Y%m%d'))
    biz_date_time = curDate
    logData = [status_code, status_desc, api_table, api_type, api_code, biz_dt_code, biz_date_time]
    ### insert data
    connDe = cx_Oracle.connect(user="de_prj", password="de_prj", dsn="192.168.21.132/orcl.localdomain")
    cursDe = connDe.cursor()
    cursDe.execute(logSql, logData)
    connDe.commit()
    connDe.close()
        
def executeIndexApi():
    # print("Call API")
    dailyIndex = callAPI() #(indexUrl, indexHost, indexKey, indexSymbol, indexRegion)
    # print("Status code " + dailyIndex.status_code)
    write_log_api_status(logPath, dailyIndex.status_code)
    status_code = dailyIndex.status_code
    if dailyIndex.status_code == 200:
        ### create DB connection
        connDe = cx_Oracle.connect(user="de_prj", password="de_prj", dsn="192.168.21.132/orcl.localdomain")
        cursDe = connDe.cursor()
        ### insert data
        curData = dailyIndex.json()
        insertToTmp(connDe, cursDe, curData, indexSymbol, indexName)
        mergeToTgt(connDe, cursDe)
        connDe.close()
    db_log(status_code)
        
    
def write_log_end(file_path):
    f = open(file_path, "a")
    endLog = "\nEnd at " + cur_time()
    f.write(endLog)
    f.close()

### Declare Dag
idxSp500Dag = DAG('Index_SP500', description='Daily Index Stock - S&P500', schedule_interval='30 22 * * *'  ,start_date =datetime(2021,11,1), catchup=False)

dagFunc_logInit = PythonOperator(task_id = 'InitLogging', python_callable=write_log_init, op_kwargs={'file_path': logPath}, dag=idxSp500Dag)
dagFunc_execute = PythonOperator(task_id = 'Execution', python_callable=executeIndexApi, dag=idxSp500Dag)
dagFunc_logEnd = PythonOperator(task_id = 'EndLogging', python_callable=write_log_end, op_kwargs={'file_path': logPath}, dag=idxSp500Dag)

dagFunc_logInit >> dagFunc_execute >> dagFunc_logEnd