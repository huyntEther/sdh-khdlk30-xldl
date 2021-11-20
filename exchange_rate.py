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
logPath = "/opt/af-logs/exchange_rate.txt"
tmpTbl = "tmp_exchange_rate"
tgtTbl = "exchange_rate"
exchangeUrl = 'https://v6.exchangerate-api.com/v6/c0a52e979663b05d1eff859e/latest/USD'

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
    callApiLog = "\nCall API for exchange rate\n"
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
    exchangeRes = requests.request("GET", exchangeUrl)
    return exchangeRes
    
def getRate(exchangeData, symbolCode):
    if exchangeData["conversion_rates"].get(symbolCode) is not None:
        lastUpdateUnix = datetime.utcfromtimestamp(exchangeData["time_last_update_unix"])
        biz_dt_code = int(lastUpdateUnix.strftime('%Y%m%d'))
        biz_date = lastUpdateUnix.replace(hour=0, minute=0, second=0, microsecond = 0)
        biz_date_time = lastUpdateUnix
        res = (symbolCode, symbolCode, symbolCode, "EXCHANGE", exchangeData["base_code"], exchangeData["conversion_rates"].get(symbolCode), biz_dt_code, biz_date, biz_date_time)
        return res
    else:
        return None
    
def insertToTmp(conn, curs, exchangeData):
    ### TRUNCATE TMP TABLE
    truncateSql = 'TRUNCATE TABLE TMP_EXCHANGE_RATE'
    curs.execute(truncateSql)
    conn.commit()
    ### prepare data:
    currencyList = ["GBP", "EUR", "CNY", "JPY", "RUB", "VND", "AUD"]
    insertData = []
    for c in currencyList:
        val = getRate(exchangeData, c)
        if val is not None:
            insertData.append(val)
    ### insert data to tmp
    insertSql = 'insert into TMP_EXCHANGE_RATE  (symbol_code	, name	, description	, indicator_type	, base_currency_code	, exchange_rate		, biz_dt_code	, biz_date	, biz_date_time) values (:symbol_code, :name, :description, :indicator_type, :base_currency_code, :exchange_rate, :biz_dt_code, :biz_date, :biz_date_time)'
    curs.executemany(insertSql, insertData)
    conn.commit()
    
def mergeToTgt(conn, curs):
    sql = ' merge into EXCHANGE_RATE tgt '
    sql += ' using TMP_EXCHANGE_RATE src '
    sql += ' on (src.symbol_code = tgt.symbol_code and src.indicator_type = tgt.indicator_type and src.base_currency_code = tgt.base_currency_code and src.biz_dt_code = tgt.biz_dt_code) '
    sql += ' when not matched then '
    sql += ' insert (symbol_code	, name	, description	, indicator_type	, base_currency_code	, exchange_rate	, exchange_measurement	, biz_dt_code	, biz_date	, biz_date_time	, exchange_revert_rate) '
    sql += ' values(src.symbol_code, src.name, src.description, src.indicator_type, src.base_currency_code, src.exchange_rate, src.exchange_measurement, src.biz_dt_code, src.biz_date, src.biz_date_time, src.exchange_revert_rate) '
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
    api_table = "EXCHANGE_RATE"
    api_type = "EXCHANGE"
    api_code = ""
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
    exchangeRes = callAPI()
    write_log_api_status(logPath, exchangeRes.status_code)
    status_code = exchangeRes.status_code
    if exchangeRes.status_code == 200:
        ### create DB connection
        connDe = cx_Oracle.connect(user="de_prj", password="de_prj", dsn="192.168.21.132/orcl.localdomain")
        cursDe = connDe.cursor()
        ### insert data
        curData = exchangeRes.json()
        insertToTmp(connDe, cursDe, curData)
        mergeToTgt(connDe, cursDe)
        connDe.close()
    db_log(status_code)
        
    
def write_log_end(file_path):
    f = open(file_path, "a")
    endLog = "\nEnd at " + cur_time()
    f.write(endLog)
    f.close()

### Declare Dag
currencyExchangeRateDag = DAG('currencyExchangeRate', description='Daily Currency Exchange Rate', schedule_interval='1 1 * * *'  ,start_date =datetime(2021,11,1), catchup=False)

dagFunc_logInit = PythonOperator(task_id = 'InitLogging', python_callable=write_log_init, op_kwargs={'file_path': logPath}, dag=currencyExchangeRateDag)
dagFunc_execute = PythonOperator(task_id = 'Execution', python_callable=executeIndexApi, dag=currencyExchangeRateDag)
dagFunc_logEnd = PythonOperator(task_id = 'EndLogging', python_callable=write_log_end, op_kwargs={'file_path': logPath}, dag=currencyExchangeRateDag)

dagFunc_logInit >> dagFunc_execute >> dagFunc_logEnd