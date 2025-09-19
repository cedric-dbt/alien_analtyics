{{ config(materialized='view') }}

with src as (
  select * from {{ source('ufo_raw','coordinates_data') }}
)

select
  country as country,
  country_code as country_code,
  latitude,
  longitude,
  round(latitude,1) as lat_bucket,
  round(longitude,1) as lon_bucket
from src
where (usa_state is null or trim(usa_state) = '')
