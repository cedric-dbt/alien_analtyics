{{ config(materialized='table') }}

with
c as (select * from {{ ref('stg_crashes') }}),
w as (select * from {{ ref('stg_weather') }})

select
  c.event_date,
  count(*) as crash_events,
  sum(coalesce(c.fatalities,0)) as total_fatalities,
  avg(w.temperature_c) as avg_temp_c,
  any_value(w.daily_summary) as any_weather_summary
from c
left join w
  on c.event_date = w.event_date
group by 1
having count(*) > 0
