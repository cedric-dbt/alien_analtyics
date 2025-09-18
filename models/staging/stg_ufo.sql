{{ config(materialized='view') }}

with base as (
  select
    DATETIME as datetime_raw,
    try_to_timestamp_ntz(DATETIME, 'MM/DD/YYYY HH24:MI') as event_datetime,
    nullif(CITY, '') as city,
    nullif(STATE, '') as state,
    nullif(COUNTRY, '') as country,
    nullif(SHAPE, '') as shape,
    case
      when typeof(DURATION_SECONDS) like 'NUMBER%' then cast(DURATION_SECONDS as double)
      when regexp_like(DURATION_SECONDS, '^[+-]?[0-9]+(\\.[0-9]+)?$') then cast(DURATION_SECONDS as double)
      else null
    end as duration_seconds,
    nullif(DURATION_HOURS_MIN, '') as duration_hours_min,
    nullif(COMMENTS, '') as comments,
    DATE_POSTED as date_posted,
    case
      when typeof(LATITUDE) like 'NUMBER%' then cast(LATITUDE as double)
      when regexp_like(LATITUDE, '^[+-]?[0-9]+(\\.[0-9]+)?$') then cast(LATITUDE as double)
      else null
    end as latitude,
    case
      when typeof(LONGITUDE) like 'NUMBER%' then cast(LONGITUDE as double)
      when regexp_like(LONGITUDE, '^[+-]?[0-9]+(\\.[0-9]+)?$') then cast(LONGITUDE as double)
      else null
    end as longitude
  from {{ source('ufo', 'UFO_SIGHTINGS_RAW') }}
),
clean as (
  -- Keep records even when coordinates are missing so downstream country/state/city
  -- enrichment and aggregations can still run. We still parse and expose latitude/longitude
  -- as nullable columns.
  select
    event_datetime,
    date_trunc('day', event_datetime) as event_date,
    city, state, country, shape, duration_seconds, duration_hours_min, comments, date_posted,
    latitude, longitude,
    case when latitude is not null then round(latitude, 1) end as lat_bucket,
    case when longitude is not null then round(longitude, 1) end as lon_bucket
  from base
  where event_datetime is not null
)
select
  {{ dbt_utils.generate_surrogate_key(['event_date','lat_bucket','lon_bucket']) }} as geo_day_key,
  *
from clean
