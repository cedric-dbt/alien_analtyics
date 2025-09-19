{{ config(materialized='view') }}

--
-- Staging view for coordinates that represent US states.
-- All rows here should represent US states; force the country and country_code
-- to canonical values to avoid accidental mixing with non-US rows in the
-- original source table.
--
with src as (
  select * from {{ source('ufo_raw','coordinates_data') }}
)

select
  usa_state as usa_state,
  usa_state_latitude as state_latitude,
  usa_state_longitude as state_longitude,
  -- Canonicalize country values for this US-states staging view. Regardless
  -- of what's present in the source, every row in this view represents a US
  -- state and must have a consistent, canonical country and country_code.
  -- Use the full country name and the ISO-like alpha-3 code requested by the
  -- user.
  'United States of America' as country,
  'USA' as country_code,
  latitude,
  longitude,
  -- lightweight bucketing for approximate join/lookups (one decimal ~= 11km)
  round(usa_state_latitude, 1) as state_lat_bucket,
  round(usa_state_longitude, 1) as state_lon_bucket,
  round(latitude, 1) as lat_bucket,
  round(longitude, 1) as lon_bucket
from src
where usa_state is not null and trim(usa_state) != ''
