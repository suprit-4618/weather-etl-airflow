from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def greet():
    print("Hello Suprit! This is your PythonOperator running inside Airflow.")


def display_date():
    print("Current Airflow Run Execution Time:", datetime.now())


with DAG(
    dag_id="python_operator_basic",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
):
    task1 = PythonOperator(task_id="greet_task", python_callable=greet)

    task2 = PythonOperator(task_id="date_task", python_callable=display_date)

    task1 >> task2
