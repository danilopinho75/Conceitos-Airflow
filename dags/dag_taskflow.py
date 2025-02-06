# %%
# bibliotecas
import logging
import requests
from datetime import datetime
from airflow.decorators import dag, task, task_group
from airflow.utils.task_group import TaskGroup
# %%
# API
API = "https://api.coinbase.com/v2/prices/spot"
# %%
# Criando dag
@dag(
    dag_id="tf-bitcoin",    
    schedule="@daily",
    start_date=datetime(2025, 2, 5),
    catchup=False
)
def main():

    transform = TaskGroup("transform")
    store = TaskGroup("store")

    @task(task_id="extract", retries=2, task_group=transform)
    def extract_bitcoin():
        return requests.get(API).json()
    
    @task(task_id="transform", task_group=transform)
    def process_bitcoin(response):
        valor = response["data"]["amount"]
        criptomoeda = response["data"]["base"]
        moeda = response["data"]["currency"]
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        processed_data = {
            "valor": valor,
            "criptomoeda": criptomoeda,
            "moeda": moeda,
            "timestamp": timestamp
        }
        return processed_data
    
    @task(task_id="store", task_group=store)
    def store_bitcoin(data):
        logging.info(f"Preço do Bitcoin: {data["valor"]}, Alteração: {data["timestamp"]}")

    store_bitcoin(process_bitcoin(extract_bitcoin()))

main()