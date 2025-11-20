from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
import requests
import csv

with DAG(
    dag_id="etl_with_retries",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
    default_args={
        "retries": 3,  # try 3 times
        "retry_delay": timedelta(seconds=20),  # wait 20s between tries
    },
):

    @task
    def extract():
        url = "https://jsonplaceholder.typicode.com/posts"
        print("Trying API request...")
        r = requests.get(url, timeout=5)
        r.raise_for_status()
        return r.json()

    @task
    def transform(posts):
        return [{"id": p["id"], "title": p["title"]} for p in posts]

    @task
    def load(cleaned_posts):
        path = "/opt/airflow/dags/etl_retries_output.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "title"])
            writer.writeheader()
            writer.writerows(cleaned_posts)
        return path

    @task
    def notify(path):
        print(f"ETL completed successfully. File saved at: {path}")

    notify(load(transform(extract())))
