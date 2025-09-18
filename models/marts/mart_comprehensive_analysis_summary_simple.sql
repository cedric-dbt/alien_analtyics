{{
  config(
    materialized='table',
    description='Simplified comprehensive summary of UFO and aviation analytics'
  )
}}

with ufo_summary as (
    select 
        'UFO_ANALYSIS' as analysis_area,
        'Total UFO Sightings' as metric_name,
        count(*) as metric_value,
        'All UFO sightings in database' as description
    from {{ ref('int_ufo_location_enriched') }}
    
    union all
    
    select 
        'UFO_ANALYSIS' as analysis_area,
        'Countries with Sightings' as metric_name,
        count(distinct country_standardized) as metric_value,
        'Number of countries with UFO reports' as description
    from {{ ref('int_ufo_location_enriched') }}
    
    union all
    
    select 
        'UFO_ANALYSIS' as analysis_area,
        'High Activity Locations' as metric_name,
        count(*) as metric_value,
        'Cities with high UFO activity levels' as description
    from {{ ref('int_ufo_location_enriched') }}
    where city_activity_level in ('VERY HIGH ACTIVITY', 'HIGH ACTIVITY')
),

aircraft_summary as (
    select 
        'AIRCRAFT_CORRELATION' as analysis_area,
        'Suspicious UFO Cases' as metric_name,
        count(*) as metric_value,
        'UFO sightings with potential aircraft correlation' as description
    from {{ ref('int_ufo_aircraft_correlation') }}
    where false_positive_likelihood in ('HIGH_SUSPICION', 'MODERATE_SUSPICION')
    
    union all
    
    select 
        'AIRCRAFT_CORRELATION' as analysis_area,
        'High Suspicion Cases' as metric_name,
        count(*) as metric_value,
        'UFO cases with high probability of being aircraft incidents' as description
    from {{ ref('int_ufo_aircraft_correlation') }}
    where false_positive_likelihood = 'HIGH_SUSPICION'
    
    union all
    
    select 
        'AIRCRAFT_CORRELATION' as analysis_area,
        'Aircraft-like Features' as metric_name,
        count(*) as metric_value,
        'UFO reports showing aircraft-like characteristics' as description
    from {{ ref('int_ufo_aircraft_correlation') }}
    where has_aircraft_like_features = true
),

weather_summary as (
    select 
        'WEATHER_AVIATION' as analysis_area,
        'Weather Risk Categories' as metric_name,
        count(*) as metric_value,
        'Different weather severity levels analyzed' as description
    from {{ ref('mart_weather_aviation_safety') }}
    where analysis_type = 'weather_severity'
    
    union all
    
    select 
        'WEATHER_AVIATION' as analysis_area,
        'Geographic Risk Areas' as metric_name,
        count(*) as metric_value,
        'Countries/regions with aviation weather risk data' as description
    from {{ ref('mart_weather_aviation_safety') }}
    where analysis_type = 'geographic_risk'
    
    union all
    
    select 
        'WEATHER_AVIATION' as analysis_area,
        'Aircraft Vulnerability Types' as metric_name,
        count(*) as metric_value,
        'Aircraft types with weather vulnerability analysis' as description
    from {{ ref('mart_weather_aviation_safety') }}
    where analysis_type = 'aircraft_vulnerability'
),

combined_summary as (
    select * from ufo_summary
    union all
    select * from aircraft_summary  
    union all
    select * from weather_summary
)

select 
    analysis_area,
    metric_name,
    metric_value,
    description,
    current_timestamp() as last_updated
from combined_summary
order by analysis_area, metric_value desc
