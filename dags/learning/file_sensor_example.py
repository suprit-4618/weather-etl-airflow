from datetime import datetime
from airflow import DAG
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="file_sensor_example",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
):
    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath="/opt/airflow/dags/sample.txt",
        poke_interval=10,  # check every 10 seconds
        timeout=60 * 5,  # wait up to 5 minutes
        mode="reschedule",  # efficient waiting
    )

    after_file = BashOperator(
        task_id="after_file_task", bash_command="echo File detected, continuing..."
    )

    wait_for_file >> after_file
