{{
  config(
    materialized='view'
  )
}}

with weather_raw as (
    select * from {{ source('ufo_raw', 'weather_historical_data') }}
),

weather_cleaned as (
    select
        -- Time parsing and standardization
        formatted_date as observation_datetime,
        
        date(formatted_date) as observation_date,
        year(formatted_date) as observation_year,
        month(formatted_date) as observation_month,
        day(formatted_date) as observation_day,
        hour(formatted_date) as observation_hour,
        
        -- Weather conditions cleaning
        trim(upper(coalesce(summary, 'UNKNOWN'))) as weather_summary,
        trim(upper(coalesce(precip_type, 'NONE'))) as precipitation_type,
        trim(upper(coalesce(daily_summary, ''))) as daily_summary_clean,
        
        -- Numeric measurements with validation
        case 
            when temperature_c between -100 and 60 then temperature_c
            else null 
        end as temperature_celsius,
        
        case 
            when apparent_temperature_c between -100 and 60 then apparent_temperature_c
            else null 
        end as apparent_temperature_celsius,
        
        case 
            when humidity between 0 and 1 then humidity * 100  -- Convert to percentage if 0-1 scale
            when humidity between 0 and 100 then humidity
            else null 
        end as humidity_percent,
        
        case 
            when wind_speed_km >= 0 and wind_speed_km <= 500 then wind_speed_km
            else null 
        end as wind_speed_kmh,
        
        case 
            when wind_bearing_degrees >= 0 and wind_bearing_degrees <= 360 then wind_bearing_degrees
            else null 
        end as wind_bearing,
        
        case 
            when visibility_km >= 0 and visibility_km <= 100 then visibility_km
            else null 
        end as visibility_km_clean,
        
        -- Fix the typo in column name and validate
        case 
            when loud_cover between 0 and 100 then loud_cover
            else null 
        end as cloud_cover_percent,
        
        case 
            when pressure_millibars between 800 and 1200 then pressure_millibars
            else null 
        end as pressure_mb,
        
        -- Weather condition categorization
        case 
            when upper(summary) like '%CLEAR%' or upper(summary) like '%SUNNY%' then 'CLEAR'
            when upper(summary) like '%CLOUD%' or upper(summary) like '%OVERCAST%' then 'CLOUDY'
            when upper(summary) like '%RAIN%' or upper(summary) like '%DRIZZLE%' then 'RAINY'
            when upper(summary) like '%SNOW%' or upper(summary) like '%SLEET%' then 'SNOWY'
            when upper(summary) like '%FOG%' or upper(summary) like '%MIST%' then 'FOGGY'
            when upper(summary) like '%STORM%' or upper(summary) like '%THUNDER%' then 'STORMY'
            when upper(summary) like '%WIND%' then 'WINDY'
            else 'OTHER'
        end as weather_category,
        
        -- Visibility categories for aviation analysis
        case 
            when visibility_km >= 10 then 'EXCELLENT'
            when visibility_km >= 5 then 'GOOD'
            when visibility_km >= 1.5 then 'MODERATE'
            when visibility_km >= 0.8 then 'POOR'
            when visibility_km < 0.8 then 'VERY_POOR'
            else 'UNKNOWN'
        end as visibility_category,
        
        -- Wind categories
        case 
            when wind_speed_km < 12 then 'CALM'
            when wind_speed_km < 25 then 'LIGHT'
            when wind_speed_km < 39 then 'MODERATE'
            when wind_speed_km < 62 then 'STRONG'
            when wind_speed_km >= 62 then 'VERY_STRONG'
            else 'UNKNOWN'
        end as wind_category,
        
        -- Data quality flags
        case 
            when formatted_date is not null then true
            else false 
        end as has_valid_datetime,
        
        case 
            when visibility_km is not null and visibility_km >= 0 then true
            else false 
        end as has_visibility_data,
        
        case 
            when wind_speed_km is not null and wind_speed_km >= 0 then true
            else false 
        end as has_wind_data
        
    from weather_raw
    where formatted_date is not null  -- Only include records with valid timestamps
)

select * from weather_cleaned
