{{ config(materialized='table') }}

-- One row per day from 1900-01-01 through today (inclusive)
with g as (
  -- Use a constant rowcount (e.g., 200k ~ 548 years), then filter
  select row_number() over(order by seq4()) - 1 as n
  from table(generator(rowcount => 200000))
)
select
  cast(dateadd(day, n, to_date('1900-01-01')) as date) as date_day
from g
where dateadd(day, n, to_date('1900-01-01')) <= current_date()