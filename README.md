# 🌦️ Weather ETL Pipeline using Apache Airflow & PostgreSQL

A complete end-to-end ETL pipeline built with **Apache Airflow 3.x**, **Docker**, **Python**, and **PostgreSQL**. This project extracts real-time weather data from the Open-Meteo API, validates it, loads it into a PostgreSQL database, and is orchestrated entirely through Airflow running in Docker.

This project is structured like a real data engineering workflow and is suitable for resume/portfolio use.

---

## 🚀 Features

- Automated ETL pipeline (extract → validate → load)
- PostgreSQL database integration
- TaskFlow API for cleaner DAGs
- Dockerized Airflow (scheduler, worker, triggerer, Redis, PostgreSQL)
- Retry logic for API or DB failure handling
- Modular folder structure
- Production-style Airflow setup

---

## 📂 Project Structure

```
airflow/
│
├── dags/
│   └── weather_pipeline/
│       ├── weather_etl.py
│       ├── weather_postgres.py
│       └── __init__.py
│
├── logs/
├── plugins/
├── config/
├── docker-compose.yaml
└── README.md
```

---

## 🧱 Architecture Flow

1. **Extract Weather Data**  
   Fetch live temperature, windspeed, wind direction, and timestamp via Open-Meteo API.

2. **Validate & Clean Data**  
   Ensures numeric types, correct field names, and no missing values.

3. **Load into PostgreSQL**  
   Inserts a new row into `weather_readings` table.

4. **Scheduled & Orchestrated via Airflow**  
   Tasks run inside Docker using CeleryExecutor.

---

## 🗄️ PostgreSQL Table Schema

```
weather_readings (
    id SERIAL PRIMARY KEY,
    temperature FLOAT,
    windspeed FLOAT,
    winddirection FLOAT,
    reading_time TIMESTAMP
)
```

---

## 🐋 How to Start Airflow + PostgreSQL (Docker)

Open terminal inside your project folder:

```
docker compose up -d
```

Airflow will start with all required services (scheduler, worker, API server, Redis, PostgreSQL).

---

## 🌐 Access Airflow UI

Go to:

```
http://localhost:8080
```

Default login:

```
username: airflow
password: airflow
```

---

## 🛠️ Running the ETL Pipeline

Inside Airflow UI:

- Locate: **weather_postgres** DAG
- Click **Trigger DAG**

A new weather record will be inserted into PostgreSQL.

---

## 🧪 Verify Loaded Data in PostgreSQL

Open an interactive PostgreSQL shell:

```
docker exec -it airflow-postgres-1 psql -U airflow -d airflow_weather
```

Then query the table:

```
SELECT * FROM weather_readings;
```

Each DAG run creates a new weather reading row.

---

## 🧰 Technologies Used

- Apache Airflow 3.x (TaskFlow API + CeleryExecutor)
- Docker & Docker Compose
- PostgreSQL 16
- Redis
- Python 3.12
- Open-Meteo Weather API

---

## 📌 Notes

- All learning/testing DAGs are kept separate to maintain a clean pipeline structure.
- The project uses a `.gitignore` to exclude logs, env files, pycache, and runtime artifacts.
- PostgreSQL connection is configured via Airflow Connections UI as:  
  - ID: `weather_postgres`
  - Host: `postgres`
  - DB: `airflow_weather`
  - Port: 5432
  - User: airflow / Password: airflow

---

## 📄 License

This project is built for learning and portfolio enhancement. Free to use or extend.

