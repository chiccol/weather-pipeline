-- fct_weather_hourly.sql
-- Fact table: hourly aggregates per city.
-- Grain: one row per (city, hour).
-- Materialized as a table for fast BI queries.

{ { config (materialized = 'table') } }

with staging as (
select * from { { ref ('stg_weather_observations') } }
)

select
city_input as city,
date_trunc ('hour', measured_at) as hour,

avg (temperature_celsius) as avg_temperature_celsius,
min (temperature_celsius) as min_temperature_celsius,
max (temperature_celsius) as max_temperature_celsius,
avg (feels_like_celsius) as avg_feels_like_celsius,

avg (humidity_percent) as avg_humidity_percent,
avg (pressure_hpa) as avg_pressure_hpa,
avg (wind_speed_ms) as avg_wind_speed_ms,
avg (cloudiness_percent) as avg_cloudiness_percent,

-- most frequent weather condition in the hour.
mode () within group (order by weather_condition) as dominant_condition,

count (*) as observation_count,
max (ingested_at) as last_ingested_at
from staging
group by 1, 2
