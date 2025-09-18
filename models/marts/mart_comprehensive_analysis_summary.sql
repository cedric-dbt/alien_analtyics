{{
  config(
    materialized='table',
    description='Comprehensive summary combining UFO debunking analysis with weather-aviation safety insights'
  )
}}

-- NOTE: This model previously raised division-by-zero errors when computing
-- percentage metrics. We guard denominators with NULLIF(denominator, 0)
-- so SQL does not error when a denominator is zero. For dashboard-friendliness
-- we COALESCE the final percentage to 0 when the value is undefined.
-- To change back to NULL (to indicate "undefined"), remove the outer COALESCE.

with ufo_debunking as (
    select * from {{ ref('mart_ufo_debunking_analysis') }}
),

weather_aviation as (
    select * from {{ ref('mart_weather_aviation_safety') }}
),

ufo_base_stats as (
    select * from {{ ref('mart_ufo_dashboard_summary') }}
),

-- High-level summary metrics
summary_metrics as (
    select 
        'overall_statistics' as metric_category,
        'total_ufo_sightings' as metric_name,
        count(*) as metric_value,
        null as metric_percentage,
        'Total UFO sightings in database' as description
    from {{ ref('int_ufo_location_enriched') }}
    
    union all
    
    select 
        'overall_statistics' as metric_category,
        'potentially_explained_sightings' as metric_name,
        count(*) as metric_value,
    coalesce(round(count(*) * 100.0 / nullif((select count(*) from {{ ref('int_ufo_location_enriched') }}), 0), 2), 0) as metric_percentage,
        'UFO sightings with potential aircraft correlation' as description
    from ufo_debunking
    where analysis_type = 'case_details' and suspicion_level in ('HIGH_SUSPICION', 'MODERATE_SUSPICION')
    
    union all
    
    select 
        'overall_statistics' as metric_category,
        'high_confidence_debunking' as metric_name,
        count(*) as metric_value,
    coalesce(round(count(*) * 100.0 / nullif((select count(*) from ufo_debunking where analysis_type = 'case_details'), 0), 2), 0) as metric_percentage,
        'High-confidence debunking cases with strong evidence' as description
    from ufo_debunking
    where analysis_type = 'case_details' and suspicion_level = 'HIGH_SUSPICION'
    
    union all
    
    select 
        'aviation_safety' as metric_category,
        'total_crashes_analyzed' as metric_name,
        sum(crash_count) as metric_value,
        null as metric_percentage,
        'Total aircraft crashes with weather data' as description
    from weather_aviation
    where analysis_type = 'weather_severity'
    
    union all
    
    select 
        'aviation_safety' as metric_category,
        'weather_attributed_crashes' as metric_name,
        sum(weather_attributed) as metric_value,
        round(sum(weather_attributed) * 100.0 / nullif(sum(crash_count), 0), 2) as metric_percentage,
        'Crashes explicitly attributed to weather conditions' as description
    from weather_aviation
    where analysis_type = 'weather_severity'
),

-- Top insights and findings using window functions
ufo_top_insights as (
    select 
        'ufo_debunking' as insight_category,
        'most_suspicious_location' as insight_type,
        location as insight_subject,
        explanation as insight_description,
        correlation_score as insight_score,
        event_date as insight_date,
        row_number() over (order by correlation_score desc) as rn
    from ufo_debunking
    where analysis_type = 'case_details' and suspicion_level = 'HIGH_SUSPICION'
),

weather_top_insights as (
    select 
        'aviation_weather' as insight_category,
        'highest_weather_risk' as insight_type,
        category as insight_subject,
        concat('Weather danger index: ', risk_score, ' with ', crash_count, ' crashes') as insight_description,
        risk_score as insight_score,
        null as insight_date,
        row_number() over (order by risk_score desc) as rn
    from weather_aviation
    where analysis_type = 'weather_severity'
),

geographic_top_insights as (
    select 
        'aviation_geographic' as insight_category,
        'most_dangerous_region' as insight_type,
        location as insight_subject,
        concat(crash_count, ' crashes with ', fatality_rate, '% average fatality rate') as insight_description,
        risk_score as insight_score,
        null as insight_date,
        row_number() over (order by risk_score desc) as rn
    from weather_aviation
    where analysis_type = 'geographic_risk'
),

key_insights as (
    select 
        insight_category,
        insight_type,
        insight_subject,
        insight_description,
        insight_score,
        insight_date
    from ufo_top_insights
    where rn <= 5
    
    union all
    
    select 
        insight_category,
        insight_type,
        insight_subject,
        insight_description,
        insight_score,
        insight_date
    from weather_top_insights
    where rn <= 5
    
    union all
    
    select 
        insight_category,
        insight_type,
        insight_subject,
        insight_description,
        insight_score,
        insight_date
    from geographic_top_insights
    where rn <= 5
),

-- Correlation between UFO sightings and aviation incidents by region
regional_correlation as (
    select 
        coalesce(u.location, w.location) as region,
        
        -- UFO metrics
        count(case when u.analysis_type = 'case_details' then 1 end) as suspicious_ufo_cases,
        count(case when u.suspicion_level = 'HIGH_SUSPICION' then 1 end) as high_suspicion_ufos,
        avg(case when u.correlation_score is not null then u.correlation_score end) as avg_ufo_correlation,
        
        -- Aviation metrics  
        sum(case when w.analysis_type = 'geographic_risk' then w.crash_count end) as total_aviation_crashes,
        avg(case when w.analysis_type = 'geographic_risk' then w.risk_score end) as avg_weather_risk,
        sum(case when w.analysis_type = 'geographic_risk' then w.weather_attributed end) as weather_crashes,
        
        -- Combined risk assessment
        case 
            when count(case when u.suspicion_level = 'HIGH_SUSPICION' then 1 end) > 5 
                 and sum(case when w.analysis_type = 'geographic_risk' then w.crash_count end) > 20 then 'HIGH_ACTIVITY_REGION'
            when count(case when u.analysis_type = 'case_details' then 1 end) > 10 
                 and sum(case when w.analysis_type = 'geographic_risk' then w.crash_count end) > 10 then 'MODERATE_ACTIVITY_REGION'
            else 'LOW_ACTIVITY_REGION'
        end as regional_activity_level
        
    from ufo_debunking u
    full outer join weather_aviation w on u.location = w.location
    where coalesce(u.location, w.location) is not null
    group by coalesce(u.location, w.location)
),

-- Final comprehensive summary
final_comprehensive_summary as (
    select 
        'summary_metrics' as data_type,
        metric_category as category,
        metric_name as subject,
        metric_value as value_1,
        metric_percentage as value_2,
        null as value_3,
        description as narrative,
        null as date_context,
        null as location_context
    from summary_metrics
    
    union all
    
    select 
        'key_insights' as data_type,
        insight_category as category,
        insight_type as subject,
        insight_score as value_1,
        null as value_2,
        null as value_3,
        insight_description as narrative,
        insight_date as date_context,
        insight_subject as location_context
    from key_insights
    
    union all
    
    select 
        'regional_analysis' as data_type,
        regional_activity_level as category,
        region as subject,
        suspicious_ufo_cases as value_1,
        total_aviation_crashes as value_2,
        avg_weather_risk as value_3,
        concat('Region with ', suspicious_ufo_cases, ' suspicious UFO cases and ', total_aviation_crashes, ' crashes') as narrative,
        null as date_context,
        region as location_context
    from regional_correlation
)

select * from final_comprehensive_summary
order by data_type, category, value_1 desc
