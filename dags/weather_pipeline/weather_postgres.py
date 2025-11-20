from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests

with DAG(
    dag_id="weather_to_postgres",
    start_date=datetime(2023, 1, 1),
    schedule="0 10 * * *",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(minutes=2)},
):

    @task
    def extract():
        url = (
            "https://api.open-meteo.com/v1/forecast"
            "?latitude=12.97&longitude=77.59&current_weather=true"
        )
        r = requests.get(url, timeout=10)
        r.raise_for_status()
        return r.json()["current_weather"]

    @task
    def load_to_postgres(data):
        hook = PostgresHook(postgres_conn_id="weather_postgres")
        query = """
            INSERT INTO weather_readings (temperature, windspeed, winddirection, reading_time)
            VALUES (%s, %s, %s, %s)
        """
        hook.run(
            query,
            parameters=[
                data["temperature"],
                data["windspeed"],
                data["winddirection"],
                data["time"],
            ],
        )
        print("Inserted into PostgreSQL.")

    weather = extract()
    load_to_postgres(weather)
