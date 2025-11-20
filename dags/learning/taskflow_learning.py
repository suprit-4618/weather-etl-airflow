from datetime import datetime
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="taskflow_learning",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
):

    @task
    def extract_numbers():
        return [10, 20, 30]

    @task
    def multiply(numbers):
        return [n * 2 for n in numbers]

    @task
    def show(result):
        print("Final Output:", result)

    show(multiply(extract_numbers()))
