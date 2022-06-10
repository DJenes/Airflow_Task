from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.S3_hook import S3Hook
import pandas as pd
import re
from sqlalchemy import text,create_engine 
from glob import glob


DEFAULT_ARGS = {
    'owner': 'Airflow',
    'depends_on_past': False,
    'start_date': datetime(2022, 6, 10),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('Json2Parq', default_args=DEFAULT_ARGS,
          schedule_interval='0 * * * *')

DB_cred ="postgresql://postgres:0000@127.0.0.1:5432/BTC_stats"

def is_word_in_text(word: str, text: str) -> bool :
    pattern = r'(^|[^\w]){}([^\w]|$)'.format(word)
    pattern = re.compile(pattern, re.IGNORECASE)
    matches = re.search(pattern, text)
    return bool(matches)

def download_from_s3(key: str, bucket_name: str) -> str:
    hook = S3Hook('minio_local')
    file_name = hook.download_file(key=key, bucket_name=bucket_name)
    return file_name

def list_files_s3(bucket_name: str) -> list :
    hook=S3Hook('minio_local')
    all_keys = hook.list_keys(bucket_name)
    return all_keys

def upload_to_minio(file: str) :   
    bucket_name = 'data-parquet'
    s3 = S3Hook('minio_local')
    s3.load_file(f"temp/{file}",
                     key=f"{file}",
                     bucket_name=bucket_name)

def delete_all_keys(keys: list, bucket_name : str):
       s3 = S3Hook('minio_local') 
       s3.delete_objects(bucket_name, keys)
       
def download_transfrom_files():
    bucket_name = 'raw-data'                   
    all_files = list_files_s3(bucket_name)
    for file in all_files:
        json_file = download_from_s3(file,bucket_name)
        name = file.split('.')
        name = name[0] 
        temp_df = pd.read_json(json_file)
        if is_word_in_text('bitnex', file):
            columns = ["tid","exchange"]
            temp_df.drop(columns, inplace=True, axis=1)    
            temp_df["exchange"] = 'Bitfinex'        
            temp_df = temp_df[["exchange", "timestamp","type","amount","price"]]
            temp_df["timestamp"] = pd.to_datetime(temp_df["timestamp"],unit='s')

        if is_word_in_text('bitmex',file):
            temp_df["exchange"] = 'Bitmex'   
            columns = ["symbol","tickDirection","trdMatchID","grossValue","homeNotional","foreignNotional"]
            temp_df.drop(columns, inplace=True, axis=1)        
            temp_df = temp_df[["exchange", "timestamp","side","size","price"]]  
            temp_df["timestamp"] = pd.to_datetime(temp_df["timestamp"],unit='s')  
            temp_df = temp_df.rename(columns = {"side":"type", "size":"amount"})
      
        if is_word_in_text('poloniex',file):
            temp_df["exchange"] = 'Poloniex'
            columns = ["globalTradeID","tradeID","orderNumber","total","orderNumber"]
            temp_df.drop(columns, inplace=True, axis=1)            
            temp_df = temp_df[["exchange", "date","type","amount","rate"]]
            temp_df["date"] = pd.to_datetime(temp_df["date"],unit='s')
            temp_df = temp_df.rename(columns = {"date":"timestamp", "rate":"price"})
        temp_df.to_parquet(f'temp/{name}.parquet')
        try:
            upload_to_minio(f'{name}.parquet')
            print(f"Uploaded {name} to MinIO.") 
        except ValueError:
                print(f"File {name} already exists.")
    stock_files = sorted(glob('temp/2022*.parquet'))
    temp_df = pd.concat((pd.read_parquet(file) for file in stock_files))
    engine = create_engine(DB_cred)
    temp_df.to_sql("statsbtc", con=engine, index=False, if_exists="append")
    delete_all_keys(all_files, bucket_name)






    
t1 = PythonOperator(
    task_id='download_n_transform',
    provide_context=True,
    python_callable=download_transfrom_files,
    dag=dag
)

                     
  