{{
  config(
    materialized='table',
    description='Lookup table mapping rounded lat/lon buckets to country and US state (from coordinates_data source)'
  )
}}

with us_coords as (
  select
    state_lat_bucket as lat_bucket,
    state_lon_bucket as lon_bucket,
    country as coord_country,
    usa_state as coord_usa_state,
    country_code
  from {{ ref('stg_coordinates_us_states') }}
  where usa_state is not null
),

non_us_coords as (
  select
    lat_bucket,
    lon_bucket,
    country as coord_country,
    null as coord_usa_state,
    country_code
  from {{ ref('stg_coordinates_non_us') }}
)

-- If multiple source rows map to the same lat/lon bucket, prefer the US-state
-- mapping when present. Use ROW_NUMBER() to pick a single canonical mapping
-- per (lat_bucket, lon_bucket).
select lat_bucket, lon_bucket, coord_country, coord_usa_state, country_code
from (
  select
    t.*,
    row_number() over (
      partition by lat_bucket, lon_bucket
      order by case when coord_usa_state is not null then 1 else 0 end desc,
               coord_country asc
    ) as rn
  from (
    select * from us_coords
    union all
    select * from non_us_coords
  ) t
) final
where rn = 1
