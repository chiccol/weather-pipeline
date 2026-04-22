-- stg_weather_observations.sql
-- Parses the raw JSONB payload into strongly-typed columns.
-- Kept as a VIEW (cheap, always fresh) — see dbt_project.yml.
--
-- OpenWeather /weather response shape (trimmed):
-- {
--   "coord":   {"lon": ..., "lat": ...},
--   "weather": [{"main": "Clouds", "description": "broken clouds", ...}],
--   "main":    {"temp": 12.3, "feels_like": 11.1, "humidity": 72, "pressure": 1015},
--   "wind":    {"speed": 3.1},
--   "clouds":  {"all": 75},
--   "dt":      1712131200,
--   "name":    "London",
--   "timezone": 0
-- }

with source as (
    select * from {{ source('raw', 'weather_observations') }}
),

parsed as (
    select
        id                                                                   as observation_id,
        city                                                                 as city_input,
        (payload ->> 'name')::text                                           as city_name_api,

        (payload -> 'coord' ->> 'lat')::float                                as latitude,
        (payload -> 'coord' ->> 'lon')::float                                as longitude,

        (payload -> 'main' ->> 'temp')::float                                as temperature_celsius,
        (payload -> 'main' ->> 'feels_like')::float                          as feels_like_celsius,
        (payload -> 'main' ->> 'humidity')::int                              as humidity_percent,
        (payload -> 'main' ->> 'pressure')::int                              as pressure_hpa,

        (payload -> 'wind' ->> 'speed')::float                               as wind_speed_ms,
        (payload -> 'clouds' ->> 'all')::int                                 as cloudiness_percent,

        (payload -> 'weather' -> 0 ->> 'main')::text                         as weather_condition,
        (payload -> 'weather' -> 0 ->> 'description')::text                  as weather_description,

        to_timestamp((payload ->> 'dt')::bigint)                             as measured_at,
        (payload ->> 'timezone')::int                                        as timezone_offset_seconds,

        ingested_at
    from source
)

select * from parsed
