from datetime import datetime
from airflow import DAG
from airflow.decorators import task

with DAG(
    dag_id="xcom_learning",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
):

    @task
    def step_one():
        value = {"name": "Suprit", "score": 95}
        return value

    @task
    def step_two(data):
        print("Received from XCom:", data)

    step_two(step_one())
