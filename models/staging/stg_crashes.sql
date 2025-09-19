{{ config(materialized='view') }}

-- Compatibility thin view: select canonical fields from the enriched `stg_airplane_crashes` staging model.
-- This avoids duplicating parsing/cleaning logic and ensures downstream models that depend on
-- `stg_crashes` continue to work.

select
  {{ dbt_utils.generate_surrogate_key(['crash_date','crash_location']) }} as location_day_key,
  crash_date as event_date,
  crash_time_raw as time_col,
  crash_location as location,
  airline_operator as operator,
  people_aboard as aboard,
  total_fatalities as fatalities,
  ground_fatalities as ground,
  crash_summary as summary,
  has_valid_date
from {{ ref('stg_airplane_crashes') }}
