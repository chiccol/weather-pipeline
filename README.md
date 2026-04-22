# Weather ELT Pipeline — Airflow + dbt + Postgres

A small ELT data pipeline I built to get hands-on with Airflow and dbt. It
ingests current weather data from the
[OpenWeather API](https://openweathermap.org/current), lands it in PostgreSQL,
and transforms it with dbt into a small star schema. The whole thing is
orchestrated by Apache Airflow and runs locally on Docker Compose.

**Tech stack:** Apache Airflow 2.10 · dbt Core 1.8 (Postgres adapter) ·
PostgreSQL 15 · Python 3.11 · Docker · Docker Compose · REST API integration ·
SQL · Dimensional modelling.

In progress!!!

---

## Architecture

```
       ┌──────────────────────┐
       │  OpenWeather REST API │
       └──────────┬────────────┘
                  │ HTTP (hourly, 7 cities)
                  ▼
   ┌──────────────────────────────────┐
   │  Airflow DAG: weather_pipeline   │
   │                                  │
   │  extract_and_load_weather        │   PythonOperator
   │            │                     │
   │            ▼                     │
   │  dbt deps → dbt run → dbt test   │   BashOperator
   └─────┬────────────────────────────┘
         │ JSONB                           ┌──────────────────────┐
         ▼                                 │   BI / analyst       │
  ┌──────────────────┐     dbt      ┌───▶  │   SELECT * FROM      │
  │  raw.weather_*   │ ──────────▶  │      │   analytics_marts.*  │
  │     (JSONB)      │              │      └──────────────────────┘
  └──────────────────┘              │
                                    │
  ┌───────────────────────────┐     │
  │ analytics_staging.stg_*   │ ──┐ │
  │  (typed views)            │   │ │
  └───────────────────────────┘   ▼ │
  ┌───────────────────────────┐     │
  │ analytics_marts.fct_*     │ ────┘
  │ analytics_marts.dim_*     │
  │  (tables, tested)         │
  └───────────────────────────┘
```

- **Extract/Load** is a `PythonOperator` that calls the API and writes raw JSON
  into `raw.weather_observations` (`JSONB` column).
- **Transform** is dbt, structured in two layers:
  - `staging/` — thin views that parse JSON into typed columns.
  - `marts/` — a fact table (`fct_weather_hourly`) and a dimension
    (`dim_cities`), materialised as tables.
- **Tests** run after every build (`dbt test`): uniqueness, not-null,
  accepted-range, and a source-freshness check.

---

## Project layout

```
weather-pipeline/
├── docker-compose.yml            # All services
├── Dockerfile.airflow            # Airflow image + dbt installed
├── requirements.txt              # Python pins
├── Makefile                      # make up / down / trigger / psql / ...
├── .env.example                  # Copy to .env
├── scripts/
│   └── init_db.sql               # Creates the "weather" DB + user
├── dags/
│   └── weather_pipeline_dag.py   # The DAG
└── dbt/
    ├── dbt_project.yml
    ├── profiles.yml              # Reads env vars, no secrets
    ├── packages.yml              # dbt_utils
    └── models/
        ├── staging/
        │   ├── _sources.yml      # declares raw.weather_observations
        │   ├── _stg_models.yml   # tests
        │   └── stg_weather_observations.sql
        └── marts/
            ├── _marts_models.yml # tests
            ├── fct_weather_hourly.sql
            └── dim_cities.sql
```

---

## Quickstart

### 1. Get an OpenWeather API key

Sign up free at <https://openweathermap.org/api>. A new key can take a few
minutes (up to a couple of hours) to activate.

### 2. Configure environment

```bash
git clone <this-repo> weather-pipeline && cd weather-pipeline
make init                       # copies .env.example → .env
# then edit .env and set OPENWEATHER_API_KEY=...
```

On Linux also set `AIRFLOW_UID=$(id -u)` in `.env` so Airflow's container user
matches yours and can write to `logs/`.

### 3. Start the stack

```bash
make up
```

This builds the custom Airflow image (installs `dbt-postgres`), migrates the
metadata DB, creates an `admin / admin` user, and starts the webserver +
scheduler. First boot takes ~2 minutes.

### 4. Open Airflow and trigger the DAG

Visit <http://localhost:8080> and log in as `admin` / `admin`. Un-pause
`weather_pipeline` (it's paused by default) — it will run hourly. To run it
immediately:

```bash
make trigger
```

### 5. Inspect the results

```bash
make psql
```

```sql
-- How many raw observations have we collected?
SELECT count(*), max(ingested_at) FROM raw.weather_observations;

-- Latest hourly averages per city
SELECT *
FROM   analytics_marts.fct_weather_hourly
ORDER  BY hour DESC, city
LIMIT  20;

-- Cities dimension
SELECT * FROM analytics_marts.dim_cities;
```

### 6. Tear down

```bash
make down     # stop but keep data
make nuke     # stop and wipe the postgres volume
```

---

## What this covers

- **Orchestration with Airflow** — DAG authoring, scheduling, retries, task
  dependencies, `PythonOperator` vs `BashOperator`, Airflow Connections via env
  vars (`AIRFLOW_CONN_*`), the `PostgresHook`.
- **ELT with dbt** — sources, staging/marts layering, refs, materialisations
  (view vs table), schema tests, `dbt_utils` tests, source freshness, env-var
  templating in `profiles.yml`, package management.
- **Dimensional modelling** — one fact + one dimension, documented grain,
  tests on the fact's composite key.
- **Containerised data stack** — multi-service Docker Compose, custom base
  image, volume mounts, healthchecks, init scripts, idempotent startup.
- **Secrets hygiene** — no credentials in code; everything via env vars; `.env`
  gitignored with a committed `.env.example`.

---

## Troubleshooting

- **`OPENWEATHER_API_KEY is not set`** — `.env` wasn't filled before `make up`,
  or the key hasn't activated yet (new keys can take up to ~2h).
- **`permission denied` on `logs/`** on Linux — set `AIRFLOW_UID=$(id -u)` in
  `.env` and `make nuke && make up`.
- **`relation "raw.weather_observations" does not exist`** in dbt — the DAG's
  first task creates it defensively, so just trigger the DAG once.
- **Init SQL didn't run** — `scripts/init_db.sql` only runs on an *empty*
  postgres volume. Use `make nuke` to reset.
- **dbt can't connect** — confirm the env vars are being passed into the
  scheduler: `docker compose exec airflow-scheduler env | grep DBT_`.
