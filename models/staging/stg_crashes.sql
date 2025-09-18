{{ config(materialized='view') }}

with base as (
  select
    DATE as date_col,
    TIME as time_col,
    LOCATION,
    OPERATOR,
    try_to_number(ABOARD) as aboard,
    try_to_number(FATALITIES) as fatalities,
    try_to_number(GROUND) as ground,
    SUMMARY,
    -- Build timestamp; TIME may be null/blank
    case
      when nullif(TIME,'') is not null
        then try_to_timestamp_ntz(to_varchar(DATE, 'MM/DD/YYYY') || ' ' || TIME, 'MM/DD/YYYY HH24:MI')
      else try_to_timestamp_ntz(to_varchar(DATE, 'MM/DD/YYYY') || ' 00:00', 'MM/DD/YYYY HH24:MI')
    end as event_datetime
  from {{ source('crashes', 'AIRPLANE_CRASHES_SINCE_1908') }}
),
clean as (
  select
    event_datetime,
    date_trunc('day', event_datetime) as event_date,
    LOCATION as location,
    OPERATOR as operator,
    aboard, fatalities, ground, SUMMARY as summary
  from base
  where event_datetime is not null
)
select
  {{ dbt_utils.generate_surrogate_key(['event_date','location']) }} as location_day_key,
  *
from clean
