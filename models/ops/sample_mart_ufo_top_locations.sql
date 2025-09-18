{{ config(materialized='table', description='Sample rows from mart_ufo_top_locations', tags=['ops']) }}

select *
from {{ ref('mart_ufo_top_locations') }}
limit 100
