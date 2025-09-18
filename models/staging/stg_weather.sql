{{ config(materialized='view') }}

-- FORMATTED_DATE is already TIMESTAMP_NTZ in Snowflake per DDL; just standardize fields
with clean as (
  select
    FORMATTED_DATE as event_datetime,
    date_trunc('day', FORMATTED_DATE) as event_date,
    SUMMARY as summary,
    PRECIP_TYPE as precip_type,
    TEMPERATURE_C as temperature_c,
    APPARENT_TEMPERATURE_C as apparent_temperature_c,
    HUMIDITY as humidity,
    WIND_SPEED_KM as wind_speed_km,
    WIND_BEARING_DEGREES as wind_bearing_degrees,
    VISIBILITY_KM as visibility_km,
    PRESSURE_MILLIBARS as pressure_millibars,
    DAILY_SUMMARY as daily_summary
  from {{ source('weather', 'WEATHER_HISTORICAL_DATA') }}
  where FORMATTED_DATE is not null
)
select * from clean
