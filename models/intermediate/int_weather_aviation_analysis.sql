{{
  config(
    materialized='table'
  )
}}

with airplane_crashes as (
    select * from {{ ref('stg_airplane_crashes') }}
),

weather_data as (
    select * from {{ ref('stg_weather_data') }}
),

-- Match crashes with weather conditions on the same date
crash_weather_matched as (
    select 
        c.crash_date,
        c.crash_year,
        c.crash_month,
        c.crash_location,
        c.crash_country,
        c.airline_operator,
        c.aircraft_type,
        c.aircraft_manufacturer,
        c.operator_type,
        c.people_aboard,
        c.total_fatalities,
        c.survivors,
        c.fatality_rate_percent,
        c.casualty_severity,
        c.aviation_era,
        c.crash_summary,
        
        -- Weather conditions on crash date
        w.observation_date as weather_date,
        w.weather_summary,
        w.weather_category,
        w.precipitation_type,
        w.temperature_celsius,
        w.humidity_percent,
        w.wind_speed_kmh,
        w.wind_category,
        w.visibility_km_clean as visibility_km,
        w.visibility_category,
        w.cloud_cover_percent,
        w.pressure_mb,
        w.daily_summary_clean,
        
        -- Weather hazard indicators
        case 
            when w.weather_category in ('STORMY', 'RAINY', 'SNOWY') then true
            else false 
        end as adverse_weather,
        
        case 
            when w.visibility_category in ('POOR', 'VERY_POOR') then true
            else false 
        end as poor_visibility,
        
        case 
            when w.wind_category in ('STRONG', 'VERY_STRONG') then true
            else false 
        end as high_winds,
        
        case 
            when w.cloud_cover_percent > 80 then true
            else false 
        end as heavy_cloud_cover,
        
        case 
            when w.precipitation_type != 'NONE' then true
            else false 
        end as precipitation_present,
        
        -- Combined weather risk score
        (case when w.weather_category in ('STORMY', 'RAINY', 'SNOWY') then 2 else 0 end +
         case when w.visibility_category in ('POOR', 'VERY_POOR') then 3 else 0 end +
         case when w.wind_category in ('STRONG', 'VERY_STRONG') then 2 else 0 end +
         case when w.cloud_cover_percent > 80 then 1 else 0 end +
         case when w.precipitation_type != 'NONE' then 1 else 0 end) as weather_risk_score,
        
        -- Weather severity categorization
        case 
            when (case when w.weather_category in ('STORMY', 'RAINY', 'SNOWY') then 2 else 0 end +
                  case when w.visibility_category in ('POOR', 'VERY_POOR') then 3 else 0 end +
                  case when w.wind_category in ('STRONG', 'VERY_STRONG') then 2 else 0 end +
                  case when w.cloud_cover_percent > 80 then 1 else 0 end +
                  case when w.precipitation_type != 'NONE' then 1 else 0 end) >= 6 then 'EXTREME'
            when (case when w.weather_category in ('STORMY', 'RAINY', 'SNOWY') then 2 else 0 end +
                  case when w.visibility_category in ('POOR', 'VERY_POOR') then 3 else 0 end +
                  case when w.wind_category in ('STRONG', 'VERY_STRONG') then 2 else 0 end +
                  case when w.cloud_cover_percent > 80 then 1 else 0 end +
                  case when w.precipitation_type != 'NONE' then 1 else 0 end) >= 4 then 'SEVERE'
            when (case when w.weather_category in ('STORMY', 'RAINY', 'SNOWY') then 2 else 0 end +
                  case when w.visibility_category in ('POOR', 'VERY_POOR') then 3 else 0 end +
                  case when w.wind_category in ('STRONG', 'VERY_STRONG') then 2 else 0 end +
                  case when w.cloud_cover_percent > 80 then 1 else 0 end +
                  case when w.precipitation_type != 'NONE' then 1 else 0 end) >= 2 then 'MODERATE'
            when (case when w.weather_category in ('STORMY', 'RAINY', 'SNOWY') then 2 else 0 end +
                  case when w.visibility_category in ('POOR', 'VERY_POOR') then 3 else 0 end +
                  case when w.wind_category in ('STRONG', 'VERY_STRONG') then 2 else 0 end +
                  case when w.cloud_cover_percent > 80 then 1 else 0 end +
                  case when w.precipitation_type != 'NONE' then 1 else 0 end) >= 1 then 'MILD'
            else 'GOOD'
        end as weather_severity,
        
        -- Weather-related crash indicators from summary text
        case 
            when upper(c.crash_summary) like '%WEATHER%' 
                 or upper(c.crash_summary) like '%STORM%'
                 or upper(c.crash_summary) like '%WIND%'
                 or upper(c.crash_summary) like '%RAIN%'
                 or upper(c.crash_summary) like '%FOG%'
                 or upper(c.crash_summary) like '%ICE%'
                 or upper(c.crash_summary) like '%VISIBILITY%' then true
            else false 
        end as weather_mentioned_in_summary,
        
        -- Data quality flag
        case 
            when w.observation_date is not null then true
            else false 
        end as has_weather_data
        
    from airplane_crashes c
    left join weather_data w 
        on c.crash_date = w.observation_date
    where c.crash_year >= 1950  -- Focus on era with better weather data
),

-- Aggregate weather impact statistics
weather_impact_summary as (
    select 
        crash_date,
        crash_year,
        crash_month,
        crash_location,
        crash_country,
        airline_operator,
        aircraft_type,
        aircraft_manufacturer,
        operator_type,
        people_aboard,
        total_fatalities,
        survivors,
        fatality_rate_percent,
        casualty_severity,
        aviation_era,
        crash_summary,
        
        -- Weather conditions
        weather_summary,
        weather_category,
        weather_severity,
        weather_risk_score,
        visibility_category,
        wind_category,
        
        -- Hazard flags
        adverse_weather,
        poor_visibility,
        high_winds,
        heavy_cloud_cover,
        precipitation_present,
        weather_mentioned_in_summary,
        
        -- Overall weather attribution
        case 
            when weather_mentioned_in_summary then 'EXPLICIT_WEATHER_CAUSE'
            when weather_severity in ('EXTREME', 'SEVERE') then 'LIKELY_WEATHER_FACTOR'
            when weather_severity = 'MODERATE' and poor_visibility then 'POSSIBLE_WEATHER_FACTOR'
            when weather_severity in ('MILD', 'GOOD') then 'MINIMAL_WEATHER_FACTOR'
            else 'UNKNOWN_WEATHER_IMPACT'
        end as weather_attribution,
        
        -- Weather-fatality correlation
        case 
            when weather_risk_score >= 6 and total_fatalities > 50 then 'HIGH_RISK_HIGH_CASUALTIES'
            when weather_risk_score >= 4 and total_fatalities > 20 then 'MODERATE_RISK_MODERATE_CASUALTIES'
            when weather_risk_score >= 2 and total_fatalities > 0 then 'LOW_RISK_SOME_CASUALTIES'
            when weather_risk_score < 2 and total_fatalities = 0 then 'GOOD_WEATHER_NO_CASUALTIES'
            else 'MIXED_CONDITIONS'
        end as weather_casualty_correlation,
        
        has_weather_data
        
    from crash_weather_matched
)

select * from weather_impact_summary
