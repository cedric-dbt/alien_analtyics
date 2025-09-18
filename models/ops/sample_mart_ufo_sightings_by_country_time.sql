{{ config(materialized='table', description='Sample rows from mart_ufo_sightings_by_country_time', tags=['ops']) }}

select *
from {{ ref('mart_ufo_sightings_by_country_time') }}
limit 100
