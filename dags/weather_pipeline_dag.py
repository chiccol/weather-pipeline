"""
weather_pipeline_dag.py
-----------------------

Hourly ELT pipeline:

    ┌──────────────────────────┐     ┌──────────┐     ┌──────────┐     ┌──────────┐
    │ extract_and_load_weather │ ──▶ │ dbt_deps │ ──▶ │ dbt_run  │ ──▶ │ dbt_test │
    └──────────────────────────┘     └──────────┘     └──────────┘     └──────────┘
             (Python)                    (Bash)          (Bash)          (Bash)

- Extract: one HTTP call per city to OpenWeather's /weather endpoint.
- Load:    insert the raw JSON payload into raw.weather_observations (JSONB).
- Transform (dbt):
    staging → stg_weather_observations    (parses JSONB into typed columns)
    marts   → fct_weather_hourly           (hourly aggregates per city)
              dim_cities                   (one row per city)
"""

from __future__ import annotations

import json
import logging
import os
from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

log = logging.getLogger(__name__)

# --- Configuration -----------------------------------------------------------
# In a real project these would come from Airflow Variables or a config file
# so they're editable without a code change.

CITIES = [
    "Berlin",
    "Amsterdam",
    "London",
    "New York",
    "Tokyo",
    "Sydney",
    "Milan",
    "Sao Paulo",
    "Cape Town",
]

OPENWEATHER_URL = "https://api.openweathermap.org/data/2.5/weather"

DBT_PROJECT_DIR = "/opt/airflow/dbt"
DBT_PROFILES_DIR = "/opt/airflow/dbt"


# --- Python task -------------------------------------------------------------
def extract_and_load_weather(**context) -> None:
    """Pull current weather for each city and upsert the raw JSON into Postgres.

    We store the payload *as JSONB*, unchanged, and let dbt do the parsing.
    """
    api_key = os.environ.get("OPENWEATHER_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENWEATHER_API_KEY is not set. Copy .env.example to .env and set your key."
        )

    hook = PostgresHook(postgres_conn_id="weather_db")
    conn = hook.get_conn()
    conn.autocommit = False
    cur = conn.cursor()

    inserted = 0
    for city in CITIES:
        try:
            resp = requests.get(
                OPENWEATHER_URL,
                params={"q": city, "appid": api_key, "units": "metric"},
                timeout=15,
            )
            resp.raise_for_status()
        except requests.RequestException as exc:
            # Fail the whole task if even one city fails — easier to debug the
            # first time round. A real pipeline would collect failures and
            # continue, or branch on partial success.
            log.error("OpenWeather call failed for %s: %s", city, exc)
            conn.rollback()
            raise

        cur.execute(
            "INSERT INTO raw.weather_observations (city, payload) VALUES (%s, %s);",
            (city, json.dumps(resp.json())),
        )
        inserted += 1
        log.info("Loaded weather for %s", city)

    conn.commit()
    cur.close()
    conn.close()
    log.info("Inserted %d rows into raw.weather_observations", inserted)


# --- DAG ---------------------------------------------------------------------
default_args = {
    "owner": "data_team",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="weather_pipeline",
    description="Hourly OpenWeather ELT → Postgres → dbt",
    default_args=default_args,
    start_date=datetime(2026, 1, 1),
    schedule="@hourly",          # cron "0 * * * *"
    catchup=False,                # don't backfill from start_date on first run
    max_active_runs=1,            # one run at a time
    tags=["weather", "dbt", "elt", "baseline"],
    doc_md=__doc__,
) as dag:

    extract_load = PythonOperator(
        task_id="extract_and_load_weather",
        python_callable=extract_and_load_weather,
    )

    # `dbt deps` installs packages declared in packages.yml. Safe to run every
    # time — it's a no-op if already installed.
    dbt_deps = BashOperator(
        task_id="dbt_deps",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt deps --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && "
            f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    extract_load >> dbt_deps >> dbt_run >> dbt_test
