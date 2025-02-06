#%%
# bibliotecas
import logging
import requests
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
# %%
# API
API = "https://api.coinbase.com/v2/prices/spot"
#%%
# todo Extract
def extract_data_bitcoin():
  return requests.get(API).json()
#%%
# todo process
def process_bitcoin(data):
  response = data.xcom_pull(task_ids="extract_data_bitcoin") 
  logging.info(response)
  valor = data["data"]["amount"]
  criptomoeda = data["data"]["base"]
  moeda = data["data"]["currency"]
  timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
  processed_data = {
    "valor": valor,
    "criptomoeda": criptomoeda,
    "moeda": moeda,
    "timestamp": timestamp
  }
  data.xcom_push(key="processed_data", value=processed_data)

#%%
# todo store
def store_bitcoin(ti):
  data = ti.xcom_pull(task_ids="process_bitcoin", key="processed_data")
  logging.info(data)
# %%
# Airflow modelo TaskFlow
with DAG(
  dag_id="cls-bitcoin",
  schedule="@daily",
  start_date=datetime(2021, 12, 1),
  catchup=False
):

  # Task 1
  extract_bitcoin_from_api = PythonOperator(
    task_id="extract_bitcoin_from_api",
    python_callable=extract_data_bitcoin
  )

  # Task 2
  process_bitcoin_from_api = PythonOperator(
    task_id="process_bitcoin_from_api",
    python_callable=process_bitcoin
  )

  # Task 3
  store_bitcoin_from_api = PythonOperator(
    task_id = "store_bitcoin_from_api",
    python_callable=store_bitcoin
  )


#%%