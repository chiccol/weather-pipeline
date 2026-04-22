-- dim_cities.sql
-- Dimension table: one row per tracked city, with coordinates from the most
-- recent observation. Classic "take the latest value" dedup pattern.

{ { config (materialized = 'table') } }

with ranked as (
select
city_input,
city_name_api,
latitude,
longitude,
ingested_at,
row_number () over (
partition by city_input
order by ingested_at desc
) as rn,
min (ingested_at) over (partition by city_input) as first_observed_at,
max (ingested_at) over (partition by city_input) as last_observed_at
from { { ref ('stg_weather_observations') } }
)

select
city_input as city,
city_name_api as canonical_name,
latitude,
longitude,
first_observed_at,
last_observed_at
from ranked
where rn = 1
