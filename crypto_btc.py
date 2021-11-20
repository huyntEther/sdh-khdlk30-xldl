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
logPath = "/opt/af-logs/crypt_btc.txt"
tmpTbl = "tmp_crytp_currency"
tgtTbl = "crytp_currency"
cryptoName = "Bitcoin"
cryptoSymbol = "BTC"
indexRegion = "USD"
indexUrl = "https://alpha-vantage.p.rapidapi.com/query"
indexHost = "alpha-vantage.p.rapidapi.com"
indexKey = "fda1f96474mshe318e95e5c2386ap15b0c7jsncb63f328a04b"

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
    callApiLog = "\nCall API for\t\t" +cryptoName +"\t\twith code " + cryptoSymbol
    f.write("\n-------------")
    f.write(callApiLog)
    curTime = "\nStart at " + cur_time()
    f.write(curTime)
    f.close()

def write_log_api_status(file_path, apiStatus):
    f = open(file_path, "a")
    f.write("\nStatus is " + str(apiStatus))
    f.close()
    
def callAPI():
    url = indexUrl
    headers = {
        'x-rapidapi-host': indexHost,
        'x-rapidapi-key': indexKey
    }
    queryString = {
        'market': indexRegion,
        'symbol': cryptoSymbol,
        'function': 'DIGITAL_CURRENCY_DAILY'
    }
    cryptoResponse = requests.request("GET", url, headers = headers, params = queryString)
    return cryptoResponse
    
    
def mergeToTgt(conn, curs):
    sql = 'MERGE INTO  crypto_currency tgt '
    sql += ' USING tmp_crypto_currency src '
    sql += ' on ( src.symbol_code = tgt.symbol_code AND src.biz_dt_code = tgt.biz_dt_code ) '
    sql += ' when not matched then '
    sql += ' insert (symbol_code, name, description, indicator_type, base_currency_code, trxn_vol, close_price, open_price, high_price, low_price, biz_dt_code, biz_date, biz_date_time, created_date, updated_date, regular_market_time, CRYPTO_REVERT_PRICE) '
    sql += ' values (src.symbol_code ,src.name ,src.description ,src.indicator_type ,src.base_currency_code ,src.trxn_vol ,src.close_price ,src.open_price ,src.high_price ,src.low_price ,src.biz_dt_code ,src.biz_date ,src.biz_date_time ,src.created_date ,src.updated_date ,src.regular_market_time, src.CRYPTO_REVERT_PRICE) '
    curs.execute(sql)
    conn.commit()

def insertToTmp(connDe, cursDe, curData, cryptoSymbol, cryptoName):
    #commonData = curData
    dailyData = curData['Time Series (Digital Currency Daily)']
    dateList = list(curData['Time Series (Digital Currency Daily)'].keys())
    insertDataList =  [] ###[(15,'x'), (2,'y')]
    ### TRUNCATE TMP TABLE
    truncateSql = 'TRUNCATE TABLE tmp_crypto_currency'
    cursDe.execute(truncateSql)
    connDe.commit()
    ### prepare data:
    for i in dateList:
        rowInsertData = ()
        rowData = dailyData[i]
        rowDateStr = i
        #shortName = dailyIndexData["quoteType"]["shortName"]
        trxn_vol = rowData['5. volume']
        close_price =  rowData['4b. close (USD)']
        open_price = rowData['1b. open (USD)']
        high_price = rowData['2b. high (USD)']
        low_price = rowData['3b. low (USD)']
        regular_market_date = datetime.strptime(i, '%Y-%m-%d').replace(hour=23,minute=59)
        regular_market_time = int(regular_market_date.strftime('%s'))
        biz_dt_code = int(regular_market_date.strftime('%Y%m%d'))
        biz_date = regular_market_date.replace(hour=0, minute=0, second=0, microsecond = 0)
        biz_date_time = datetime.utcfromtimestamp(regular_market_time)
        # generate final row data
        rowInsertData = (cryptoSymbol, cryptoName, cryptoName, "CRYPTO", "USD", trxn_vol, close_price, open_price, high_price, low_price, biz_dt_code, biz_date, biz_date_time, regular_market_time)
        insertDataList.append(rowInsertData)
    ### insert data to tmp
    insertSql = 'insert into tmp_crypto_currency  (symbol_code, name, description, indicator_type, base_currency_code, trxn_vol, close_price, open_price, high_price, low_price, biz_dt_code, biz_date, biz_date_time, regular_market_time) values (:symbol_code, :name, :description, :indicator_type, :base_currency_code, :trxn_vol, :close_price, :open_price, :high_price, :low_price, :biz_dt_code, :biz_date, :biz_date_time,  :regular_market_time)'
    cursDe.executemany(insertSql, insertDataList)
    connDe.commit()

def db_log(statusCode):
    ### prepare data
    status_code = statusCode
    logSql = 'insert into api_daily_log (status_code	, status_desc	, api_table	, api_type	, api_code	, biz_dt_code	,  biz_date_time) values (:status_code	, :status_desc, :api_table	, :api_type	, :api_code	, :biz_dt_code	,  :biz_date_time)'
    if status_code == 200:
        status_desc = "SUCCESS"
    else:
        status_desc = "FAIL"
    api_table = "CRYPTO_CURRENCY"
    api_type = "CRYPTO"
    api_code = cryptoSymbol
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
    # print("Call API") cryptoRes
    cryptoRes = callAPI()
    write_log_api_status(logPath, cryptoRes.status_code)
    status_code = cryptoRes.status_code
    if cryptoRes.status_code == 200:
        ### create DB connection
        connDe = cx_Oracle.connect(user="de_prj", password="de_prj", dsn="192.168.21.132/orcl.localdomain")
        cursDe = connDe.cursor()
        ### insert data
        curData = cryptoRes.json()
        insertToTmp(connDe, cursDe, curData, cryptoSymbol, cryptoName)
        mergeToTgt(connDe, cursDe)
        connDe.close()
    db_log(status_code)
        
    
def write_log_end(file_path):
    f = open(file_path, "a")
    endLog = "\nEnd at " + cur_time()
    f.write(endLog)
    f.close()

### Declare Dag
cryptoBtcDag = DAG('Crypto_Bitcoin', description='Daily Crypto Currency - BTC', schedule_interval='0 7 * * *'  ,start_date =datetime(2021,11,15), catchup=False)

dagFunc_logInit = PythonOperator(task_id = 'InitLogging', python_callable=write_log_init, op_kwargs={'file_path': logPath}, dag=cryptoBtcDag)
dagFunc_execute = PythonOperator(task_id = 'Execution', python_callable=executeIndexApi, dag=cryptoBtcDag)
dagFunc_logEnd = PythonOperator(task_id = 'EndLogging', python_callable=write_log_end, op_kwargs={'file_path': logPath}, dag=cryptoBtcDag)

dagFunc_logInit >> dagFunc_execute >> dagFunc_logEnd