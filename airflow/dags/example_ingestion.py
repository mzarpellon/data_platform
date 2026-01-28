from airflow import DAG 
from airflow.operators.python import PythonOperator
from datetime import datetime

def hello():
    print("Airflow está funcionando corretamente.")

with DAG(
    dag_id="example_ingestion",
    start_date=datetime(2025, 1, 28),
    schedule_interval=None,
    catchup=False,
    tags=["Teste"]
):
    task=PythonOperator(
        task_id="hello_task",
        python_callable=hello
    )