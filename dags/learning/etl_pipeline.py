from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import requests
import csv

with DAG(
    dag_id="etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
):

    @task
    def extract():
        url = "https://jsonplaceholder.typicode.com/posts"
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        return response.json()

    @task
    def transform(posts):
        # Keep only id, title fields
        cleaned = [{"id": p["id"], "title": p["title"]} for p in posts]
        return cleaned

    @task
    def load(cleaned_posts):
        filepath = "/opt/airflow/dags/etl_output.csv"
        with open(filepath, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "title"])
            writer.writeheader()
            writer.writerows(cleaned_posts)
        return filepath

    @task
    def notify(path):
        print("ETL Pipeline Completed! File saved at:", path)

    notify(load(transform(extract())))
