-- scripts/init_db.sql
-- Runs ONCE as the entrypoint (docker-entrypoint-initdb.d contract.)
-- One postgres server with 1 airflow DB for its metadata and one for everything dbt related (weather)

CREATE USER weather WITH PASSWORD 'weather' ;
CREATE DATABASE weather OWNER weather ;
GRANT ALL PRIVILEGES ON DATABASE weather TO weather ;
\c weather
GRANT ALL ON SCHEMA public TO weather;

-- dbt will build schemas like analytics_staging / analytics_marts.
-- We pre-create the raw schema that the Airflow task writes into.
CREATE SCHEMA IF NOT EXISTS raw AUTHORIZATION weather;

