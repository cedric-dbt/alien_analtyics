{{
  config(
    materialized='table',
    description='Analysis of how weather conditions impact aviation safety and crash outcomes'
  )
}}

with weather_aviation_analysis as (
    select * from {{ ref('int_weather_aviation_analysis') }}
),

-- Weather impact on crash severity
weather_severity_analysis as (
    select 
        weather_severity,
        weather_category,
        count(*) as total_crashes,
        sum(total_fatalities) as total_deaths,
        sum(people_aboard) as total_people_aboard,
        avg(fatality_rate_percent) as avg_fatality_rate,
        
        -- Casualty distribution
        count(case when casualty_severity = 'NO_FATALITIES' then 1 end) as no_fatality_crashes,
        count(case when casualty_severity = 'LOW_CASUALTIES' then 1 end) as low_casualty_crashes,
        count(case when casualty_severity = 'MODERATE_CASUALTIES' then 1 end) as moderate_casualty_crashes,
        count(case when casualty_severity = 'HIGH_CASUALTIES' then 1 end) as high_casualty_crashes,
        count(case when casualty_severity = 'MASS_CASUALTY' then 1 end) as mass_casualty_crashes,
        
        -- Weather attribution
        count(case when weather_attribution = 'EXPLICIT_WEATHER_CAUSE' then 1 end) as explicit_weather_crashes,
        count(case when weather_attribution = 'LIKELY_WEATHER_FACTOR' then 1 end) as likely_weather_crashes,
        count(case when weather_attribution = 'POSSIBLE_WEATHER_FACTOR' then 1 end) as possible_weather_crashes,
        
        -- Calculate weather danger index
        round(avg(fatality_rate_percent) * (count(case when weather_attribution in ('EXPLICIT_WEATHER_CAUSE', 'LIKELY_WEATHER_FACTOR') then 1 end) * 100.0 / nullif(count(*), 0)), 2) as weather_danger_index
        
    from weather_aviation_analysis
    where has_weather_data = true
    group by weather_severity, weather_category
),

-- Temporal weather patterns
weather_temporal_patterns as (
    select 
        crash_year,
        crash_month,
        aviation_era,
        
        count(*) as total_crashes,
        count(case when adverse_weather then 1 end) as adverse_weather_crashes,
        count(case when poor_visibility then 1 end) as poor_visibility_crashes,
        count(case when high_winds then 1 end) as high_wind_crashes,
        count(case when weather_mentioned_in_summary then 1 end) as weather_explicit_crashes,
        
        avg(weather_risk_score) as avg_weather_risk,
        avg(total_fatalities) as avg_fatalities,
        sum(total_fatalities) as total_fatalities_year,
        
        -- Weather crash percentage by time period
        round(count(case when adverse_weather then 1 end) * 100.0 / nullif(count(*), 0), 2) as adverse_weather_crash_pct,
        round(count(case when weather_attribution in ('EXPLICIT_WEATHER_CAUSE', 'LIKELY_WEATHER_FACTOR') then 1 end) * 100.0 / nullif(count(*), 0), 2) as weather_attributed_crash_pct
        
    from weather_aviation_analysis
    where has_weather_data = true
    group by crash_year, crash_month, aviation_era
),

-- Aircraft type weather vulnerability
aircraft_weather_vulnerability as (
    select 
        aircraft_manufacturer,
        aircraft_type,
        operator_type,
        
        count(*) as total_crashes,
        count(case when adverse_weather then 1 end) as adverse_weather_crashes,
        count(case when weather_attribution in ('EXPLICIT_WEATHER_CAUSE', 'LIKELY_WEATHER_FACTOR') then 1 end) as weather_caused_crashes,
        
        avg(fatality_rate_percent) as avg_fatality_rate,
        avg(weather_risk_score) as avg_weather_risk_exposure,
        
        -- Weather vulnerability score
        round((count(case when weather_attribution in ('EXPLICIT_WEATHER_CAUSE', 'LIKELY_WEATHER_FACTOR') then 1 end) * 100.0 / nullif(count(*), 0)) * 
              (avg(fatality_rate_percent) / 100.0), 2) as weather_vulnerability_score,
        
        -- Most common weather conditions for this aircraft
        mode(weather_category) as most_common_weather_condition,
        mode(weather_severity) as most_common_weather_severity
        
    from weather_aviation_analysis
    where has_weather_data = true
    group by aircraft_manufacturer, aircraft_type, operator_type
    having count(*) >= 5  -- Only include aircraft types with sufficient data
),

-- Geographic weather risk analysis
geographic_weather_risk as (
    select 
        crash_country,
        
        count(*) as total_crashes,
        count(case when adverse_weather then 1 end) as adverse_weather_crashes,
        count(case when poor_visibility then 1 end) as poor_visibility_crashes,
        count(case when high_winds then 1 end) as high_wind_crashes,
        
        avg(weather_risk_score) as avg_weather_risk,
        avg(total_fatalities) as avg_fatalities_per_crash,
        sum(total_fatalities) as total_fatalities,
        
        -- Regional weather patterns
        mode(weather_category) as predominant_weather,
        mode(weather_severity) as typical_weather_severity,
        
        -- Risk metrics
        round(count(case when weather_attribution in ('EXPLICIT_WEATHER_CAUSE', 'LIKELY_WEATHER_FACTOR') then 1 end) * 100.0 / nullif(count(*), 0), 2) as weather_crash_percentage,
        round(avg(fatality_rate_percent), 2) as avg_fatality_rate,
        
        -- Geographic weather danger score
        round((avg(weather_risk_score) * avg(fatality_rate_percent)) / 100.0, 2) as geographic_weather_danger_score
        
    from weather_aviation_analysis
    where has_weather_data = true and crash_country is not null
    group by crash_country
    having count(*) >= 10  -- Only include countries with sufficient crash data
),

-- Final combined dashboard data
final_weather_aviation_analysis as (
    select 
        'weather_severity' as analysis_type,
        weather_severity as category,
        weather_category as subcategory,
        total_crashes as crash_count,
        total_deaths as fatality_count,
        avg_fatality_rate as fatality_rate,
        weather_danger_index as risk_score,
        explicit_weather_crashes as weather_attributed,
        null as time_period,
        null as location
    from weather_severity_analysis
    
    union all
    
    select 
        'temporal_trends' as analysis_type,
        aviation_era as category,
        crash_year::string as subcategory,
        total_crashes as crash_count,
        total_fatalities_year as fatality_count,
        adverse_weather_crash_pct as fatality_rate,
        avg_weather_risk as risk_score,
        weather_explicit_crashes as weather_attributed,
        crash_month as time_period,
        null as location
    from weather_temporal_patterns
    
    union all
    
    select 
        'aircraft_vulnerability' as analysis_type,
        aircraft_manufacturer as category,
        aircraft_type as subcategory,
        total_crashes as crash_count,
        null as fatality_count,
        avg_fatality_rate as fatality_rate,
        weather_vulnerability_score as risk_score,
        weather_caused_crashes as weather_attributed,
        null as time_period,
        null as location
    from aircraft_weather_vulnerability
    
    union all
    
    select 
        'geographic_risk' as analysis_type,
        crash_country as category,
        predominant_weather as subcategory,
        total_crashes as crash_count,
        total_fatalities as fatality_count,
        avg_fatality_rate as fatality_rate,
        geographic_weather_danger_score as risk_score,
        adverse_weather_crashes as weather_attributed,
        null as time_period,
        crash_country as location
    from geographic_weather_risk
)

select * from final_weather_aviation_analysis
order by analysis_type, risk_score desc, crash_count desc
