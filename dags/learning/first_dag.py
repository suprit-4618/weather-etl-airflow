from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="my_first_dag",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
):
    task1 = BashOperator(
        task_id="print_date", bash_command="echo Airflow 3 is working!"
    )
