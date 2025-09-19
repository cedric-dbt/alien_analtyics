{{ config(materialized='view') }}

with base as (
  select
    datetime as datetime_raw,
    {{ coerce_to_timestamp('datetime', 'date_posted') }} as event_datetime,
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
  date_posted as date_posted,
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
  from {{ source('ufo_raw','ufo_sightings_raw') }}
),
clean as (
  -- Keep records even when coordinates are missing so downstream country/state/city
  -- enrichment and aggregations can still run. We still parse and expose latitude/longitude
  -- as nullable columns.
  select
    coalesce(event_datetime, cast(date_posted as timestamp_ntz)) as event_datetime,
    case when coalesce(event_datetime, cast(date_posted as timestamp_ntz)) is not null
      then date_trunc('day', coalesce(event_datetime, cast(date_posted as timestamp_ntz)))
      else null end as event_date,
    city, state, country, shape, duration_seconds, duration_hours_min, comments, date_posted,
    latitude, longitude,
    case when latitude is not null then round(latitude, 1) end as lat_bucket,
    case when longitude is not null then round(longitude, 1) end as lon_bucket
  from base
)
,
coords as (
  select
    country as coord_country,
    usa_state as coord_usa_state,
    round(latitude, 1) as lat_bucket,
    round(longitude, 1) as lon_bucket
  from {{ source('ufo_raw','coordinates_data') }}
),

enriched as (
  select
    c.*,
    coalesce(nullif(c.country,''), nullif(cd.coord_country,'')) as country_enriched,
    coalesce(nullif(c.state,''), nullif(cd.coord_usa_state,'')) as state_enriched
  from clean c
  left join coords cd
    on c.lat_bucket = cd.lat_bucket and c.lon_bucket = cd.lon_bucket
)
select
  {{ dbt_utils.generate_surrogate_key(['event_date','lat_bucket','lon_bucket']) }} as geo_day_key,
  event_datetime,
  event_date,
  city,
  state_enriched as state,
  country_enriched as country,
  shape,
  duration_seconds,
  duration_hours_min,
  comments,
  date_posted,
  latitude,
  longitude
from enriched
