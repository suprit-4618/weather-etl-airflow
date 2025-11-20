# Weather ETL Pipeline using Apache Airflow & PostgreSQL

This project implements a complete automated ETL pipeline using Apache Airflow, Docker, Python, and PostgreSQL. It fetches real-time weather data from the Open-Meteo public API, validates and cleans the response, and loads the final processed data into a PostgreSQL database running inside Docker. The entire orchestration runs on Airflow 3.x with a TaskFlow-based DAG, making the project production-style and fully portable for real data engineering use cases. The project structure is clean, modular, and optimized.

airflow/
│
├── dags/
│   ├── weather_pipeline/
│   │      ├── weather_etl.py
│   │      ├── weather_to_postgres.py
│   │      └── __init__.py
│   └── learning/
│
├── logs/
├── plugins/
├── config/
└── docker-compose.yaml

The pipeline works in three core steps: extraction of weather data using an HTTP request to Open-Meteo, transformation and validation through Airflow TaskFlow functions, and loading into a SQL table named weather_readings inside the airflow_weather PostgreSQL database. The table structure is:

weather_readings (
    id SERIAL PRIMARY KEY,
    temperature FLOAT,
    windspeed FLOAT,
    winddirection FLOAT,
    reading_time TIMESTAMP
)

To run the project, start the entire Airflow environment using Docker Compose with: docker compose up -d. Then open the Airflow web UI at http://localhost:8080 and log in using username airflow and password airflow. Inside the UI, trigger the DAG named weather_to_postgres to run the ETL. Once the DAG finishes, you can query the PostgreSQL database using:

docker exec -it airflow-postgres-1 psql -U airflow -d airflow_weather

Inside the psql shell, run:

SELECT * FROM weather_readings;

Each run of the DAG inserts a fresh weather reading into the database, allowing historical data accumulation. This project demonstrates end-to-end orchestration, database integration, clean pipeline structuring, Docker-based containerized execution, and Airflow DAG development.
