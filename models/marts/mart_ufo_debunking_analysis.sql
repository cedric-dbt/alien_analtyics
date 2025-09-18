{{
  config(
    materialized='table',
    description='Analysis of UFO sightings that may be explained by aircraft incidents - the debunking dashboard'
  )
}}

with ufo_aircraft_correlation as (
    select * from {{ ref('int_ufo_aircraft_correlation') }}
),

ufo_base_data as (
    select * from {{ ref('int_ufo_location_enriched') }}
),

-- High suspicion UFO cases
high_suspicion_cases as (
    select 
        sighting_id,
        sighting_datetime,
        sighting_date,
        ufo_country,
        ufo_state,
        ufo_city,
        ufo_shape,
        ufo_duration,
        closest_crash_days,
        false_positive_likelihood,
        max_correlation_score,
        nearby_crashes_count,
        crashes_within_week,
        crashes_within_month,
        has_aircraft_like_features,
        
        -- Evidence strength
        case 
            when crashes_within_week > 0 and has_aircraft_like_features then 'STRONG_EVIDENCE'
            when crashes_within_week > 0 or (crashes_within_month > 0 and has_aircraft_like_features) then 'MODERATE_EVIDENCE'
            when crashes_within_month > 0 then 'WEAK_EVIDENCE'
            else 'NO_EVIDENCE'
        end as debunking_evidence_strength,
        
        -- Create explanation narrative
        case 
            when crashes_within_week > 0 and has_aircraft_like_features 
            then 'UFO sighting occurred within ' || closest_crash_days || ' days of aircraft crash. Sighting characteristics match aircraft debris/explosion.'
            when crashes_within_week > 0 
            then 'UFO sighting occurred within ' || closest_crash_days || ' days of aircraft crash in nearby area.'
            when crashes_within_month > 0 and has_aircraft_like_features 
            then 'UFO sighting shows aircraft-like characteristics and occurred within ' || closest_crash_days || ' days of aviation incident.'
            else 'Temporal/spatial correlation with aviation incident detected.'
        end as potential_explanation
        
    from ufo_aircraft_correlation
    where false_positive_likelihood in ('HIGH_SUSPICION', 'MODERATE_SUSPICION')
),

-- Summary statistics by region and time
debunking_summary_stats as (
    select 
        ufo_country,
        ufo_state,
        date_trunc('year', sighting_date) as sighting_year,
        date_trunc('month', sighting_date) as sighting_month,
        
        count(*) as total_suspicious_sightings,
        count(case when false_positive_likelihood = 'HIGH_SUSPICION' then 1 end) as high_suspicion_count,
        count(case when false_positive_likelihood = 'MODERATE_SUSPICION' then 1 end) as moderate_suspicion_count,
        0 as strong_evidence_count,
        count(case when has_aircraft_like_features then 1 end) as aircraft_like_features_count,
        
        avg(closest_crash_days) as avg_days_to_crash,
        avg(nearby_crashes_count) as avg_nearby_crashes,
        sum(crashes_within_week) as total_crashes_within_week,
        sum(crashes_within_month) as total_crashes_within_month,
        
        -- Calculate debunking rate
        round(count(case when false_positive_likelihood in ('HIGH_SUSPICION', 'MODERATE_SUSPICION') then 1 end) * 100.0 / 
              nullif(count(*), 0), 2) as potential_false_positive_rate
        
    from ufo_aircraft_correlation
    group by ufo_country, ufo_state, date_trunc('year', sighting_date), date_trunc('month', sighting_date)
),

-- Most convincing debunking cases
top_debunking_cases as (
    select 
        *,
        row_number() over (order by max_correlation_score desc, closest_crash_days asc) as debunking_rank
    from high_suspicion_cases
    where false_positive_likelihood in ('HIGH_SUSPICION', 'MODERATE_SUSPICION')
),

-- Aircraft type correlation
aircraft_type_analysis as (
    select 
        closest_aircraft_type,
        count(*) as correlated_ufo_sightings,
        count(case when false_positive_likelihood = 'HIGH_SUSPICION' then 1 end) as high_suspicion_sightings,
        avg(closest_crash_days) as avg_days_between,
        avg(closest_crash_fatalities) as avg_crash_fatalities,
        
        -- Most common UFO shapes for this aircraft type
        mode(ufo_shape) as most_common_ufo_shape,
        avg(ufo_duration) as avg_ufo_duration
        
    from ufo_aircraft_correlation
    where closest_aircraft_type is not null
    group by closest_aircraft_type
    having count(*) >= 3  -- Only include aircraft types with multiple correlations
),

-- Final combined results
final_debunking_analysis as (
    select 
        'case_details' as analysis_type,
        sighting_id as record_id,
        sighting_date as event_date,
        ufo_country as location,
        ufo_state as sub_location,
        false_positive_likelihood as suspicion_level,
        null as evidence_strength,
        potential_explanation as explanation,
        closest_crash_days as days_to_incident,
        null as related_aircraft,
        max_correlation_score as correlation_score,
        null as summary_metric
    from high_suspicion_cases
    
    union all
    
    select 
        'regional_summary' as analysis_type,
        concat(ufo_country, '|', ufo_state) as record_id,
        sighting_year as event_date,
        ufo_country as location,
        ufo_state as sub_location,
        null as suspicion_level,
        null as evidence_strength,
        concat('Region shows ', potential_false_positive_rate, '% potential false positive rate') as explanation,
        avg_days_to_crash as days_to_incident,
        null as related_aircraft,
        null as correlation_score,
        potential_false_positive_rate as summary_metric
    from debunking_summary_stats
    
    union all
    
    select 
        'aircraft_correlation' as analysis_type,
        closest_aircraft_type as record_id,
        null as event_date,
        null as location,
        null as sub_location,
        null as suspicion_level,
        null as evidence_strength,
        concat(correlated_ufo_sightings, ' UFO sightings correlated with ', closest_aircraft_type, ' incidents') as explanation,
        avg_days_between as days_to_incident,
        null as related_aircraft,
        null as correlation_score,
        high_suspicion_sightings as summary_metric
    from aircraft_type_analysis
)

select * from final_debunking_analysis
order by analysis_type, correlation_score desc, days_to_incident asc
