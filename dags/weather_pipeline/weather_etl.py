from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import BranchPythonOperator
import requests
import csv

with DAG(
    dag_id="weather_etl_pipeline",
    start_date=datetime(2023, 1, 1),
    schedule="0 9 * * *",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
):

    # STEP 1 — Extract
    @task
    def extract():
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=12.97&longitude=77.59&current_weather=true"
        )
        print("Fetching weather data...")
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()

    # STEP 2 — Clean
    @task
    def clean(data):
        cw = data["current_weather"]
        cleaned = {
            "temperature": cw["temperature"],
            "windspeed": cw["windspeed"],
            "winddirection": cw["winddirection"],
            "time": cw["time"],
        }
        return cleaned

    # STEP 3 — Decide (branch)
    def validate(data):
        cw = data.get("current_weather")
        if not cw:
            return "bad_data"

        temp = cw.get("temperature")
        wind = cw.get("windspeed")

        if temp is None or wind is None:
            return "bad_data"

        if temp < -50 or temp > 60:
            return "bad_data"

        return "good_data"

    @task
    def bad_data():
        print("BAD DATA — pipeline stopped.")
        return "stopped"

    @task
    def good_data(cleaned):
        print("Good data:", cleaned)
        return cleaned

    # STEP 4 — Load
    @task
    def load(cleaned):
        path = "/opt/airflow/dags/weather_data.csv"
        with open(path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(
                f, fieldnames=["temperature", "windspeed", "winddirection", "time"]
            )
            writer.writeheader()
            writer.writerow(cleaned)
        return path

    # STEP 5 — Notify
    @task
    def notify(path):
        print("Success. File saved at:", path)

    # ----- WIRING -----
    extracted = extract()
    decision = BranchPythonOperator(
        task_id="branch_decision",
        python_callable=validate,
        op_args=[extracted],
    )

    cleaned = clean(extracted)
    good = good_data(cleaned)
    loaded = load(good)
    notified = notify(loaded)

    decision >> bad_data()
    decision >> good >> loaded >> notified
