from datetime import datetime
from airflow import DAG
from airflow.decorators import task
import requests
import csv

with DAG(
    dag_id="etl_with_quality_check",
    start_date=datetime(2023, 1, 1),
    schedule="@daily",
    catchup=False,
):

    @task
    def extract():
        url = "https://jsonplaceholder.typicode.com/posts"
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()

    @task
    def transform(posts):
        cleaned = [{"id": p["id"], "title": p["title"]} for p in posts]
        return cleaned

    @task
    def quality_check(cleaned_posts):
        # Check 1: Not empty
        if len(cleaned_posts) == 0:
            raise ValueError("Data is empty! Failing the pipeline.")

        # Check 2: Required fields exist
        for row in cleaned_posts:
            if "id" not in row or "title" not in row:
                raise ValueError("Missing required fields in data!")

        # Check 3: Titles must be strings
        for row in cleaned_posts:
            if not isinstance(row["title"], str):
                raise ValueError("Title field is not a string!")

        print("Data Quality Checks Passed!")
        return cleaned_posts

    @task
    def load(cleaned_posts):
        path = "/opt/airflow/dags/etl_output_checked.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=["id", "title"])
            writer.writeheader()
            writer.writerows(cleaned_posts)
        return path

    @task
    def notify(path):
        print("ETL Completed Successfully. File:", path)

    # Pipeline flow
    notify(load(quality_check(transform(extract()))))
