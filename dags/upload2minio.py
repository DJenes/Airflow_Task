from datetime import datetime, timedelta
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import json

DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('BitcoinData2Minio', default_args=DEFAULT_ARGS,
          schedule_interval='*/1 * * * *')


def write_bitnex(ts: str, **_):
    bitnex = requests.get("https://api.bitfinex.com/v1/trades/btcusd?limit_trades=500")
    
    data_bitnex = bitnex.json()    
    bucket_name = 'raw-data'
    s3 = S3Hook('minio_local')
    s3.load_string(
        string_data=json.dumps(data_bitnex),
        key=f"{ts}-bitnex.json",
        bucket_name=bucket_name)

def write_bitmex(ts: str, **_):
    bitmex = requests.get("https://www.bitmex.com/api/v1/trade?symbol=XBTUSD&count=500&reverse=true")
    
    data_bitmex = bitmex.json()
    bucket_name = 'raw-data'
    s3 = S3Hook('minio_local')
    s3.load_string(
        string_data=json.dumps(data_bitmex),
        key=f"{ts}-bitmex.json",
        bucket_name=bucket_name)

def write_poloniex(ts: str, **_):
    poloniex = requests.get("https://poloniex.com/public?command=returnTradeHistory&currencyPair=USDT_BTC")
    data_poloniex = poloniex.json() 
    bucket_name = 'raw-data'
    s3 = S3Hook('minio_local')
    s3.load_string(
        string_data=json.dumps(data_poloniex),
        key=f"{ts}-poloniex.json",
        bucket_name=bucket_name)

t1 = PythonOperator(
    task_id='bitnex_data',
    provide_context=True,
    python_callable=write_bitnex,
    dag=dag
)
t2 = PythonOperator(
    task_id='bitmex_data',
    provide_context=True,
    python_callable=write_bitmex,
    dag=dag
)
t3 = PythonOperator(
    task_id='poloniex_data',
    provide_context=True,
    python_callable=write_poloniex,
    dag=dag
)
