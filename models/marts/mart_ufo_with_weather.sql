{{ config(materialized='table') }}

with
u as (select * from {{ ref('stg_ufo') }}),
w as (select * from {{ ref('stg_weather') }})

-- Join on day to guarantee overlap; weather adds daily context
select
  u.event_date,
  count(*) as ufo_sightings,
  avg(u.duration_seconds) as avg_duration_seconds,
  avg(w.temperature_c) as avg_temp_c,
  avg(w.humidity) as avg_humidity,
  avg(w.wind_speed_km) as avg_wind_speed_km,
  any_value(w.daily_summary) as any_weather_summary
from u
left join w
  on u.event_date = w.event_date
group by 1
having count(*) > 0
