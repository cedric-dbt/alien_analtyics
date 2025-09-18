{{
  config(
    materialized='table',
    description='Simplified UFO debunking analysis showing potential false positives'
  )
}}

with ufo_aircraft_correlation as (
    select * from {{ ref('int_ufo_aircraft_correlation') }}
)

select 
    sighting_id,
    sighting_date,
    ufo_country,
    ufo_state,
    ufo_city,
    ufo_shape,
    false_positive_likelihood,
    nearby_crashes_count,
    crashes_within_week,
    crashes_within_month,
    closest_crash_days,
    max_correlation_score,
    has_aircraft_like_features,
    
    case 
        when crashes_within_week > 0 and has_aircraft_like_features 
        then 'High probability false positive - aircraft incident within week with matching characteristics'
        when crashes_within_week > 0 
        then 'Moderate probability false positive - aircraft incident within week'
        when crashes_within_month > 0 and has_aircraft_like_features 
        then 'Low probability false positive - aircraft incident within month with matching characteristics'
        else 'Minimal correlation with aircraft incidents'
    end as debunking_assessment

from ufo_aircraft_correlation
where false_positive_likelihood in ('HIGH_SUSPICION', 'MODERATE_SUSPICION', 'LOW_SUSPICION')
order by max_correlation_score desc, closest_crash_days asc
