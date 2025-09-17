{{
  config(
    materialized='table'
  )
}}

with ufo_sightings as (
    select * from {{ ref('stg_ufo_sightings') }}
),

cleaned_sightings as (
    select
        -- Parse and clean datetime
        case 
            when datetime is not null and datetime != '' 
            then try_to_timestamp(datetime)
            else null 
        end as sighting_datetime,
        
        -- Extract date components for easier filtering
        date(try_to_timestamp(datetime)) as sighting_date,
        year(try_to_timestamp(datetime)) as sighting_year,
        month(try_to_timestamp(datetime)) as sighting_month,
        dayofweek(try_to_timestamp(datetime)) as sighting_day_of_week,
        quarter(try_to_timestamp(datetime)) as sighting_quarter,
        
        -- Location cleaning
        trim(upper(coalesce(city, 'UNKNOWN'))) as city_clean,
        trim(upper(coalesce(state, 'UNKNOWN'))) as state_clean,
        trim(upper(coalesce(country, 'UNKNOWN'))) as country_clean,
        
        -- Standardize country names
        case 
            when trim(upper(country)) in ('US', 'USA', 'UNITED STATES') then 'UNITED STATES'
            when trim(upper(country)) in ('UK', 'UNITED KINGDOM', 'ENGLAND', 'SCOTLAND', 'WALES') then 'UNITED KINGDOM'
            when trim(upper(country)) in ('CA', 'CANADA') then 'CANADA'
            when trim(upper(country)) in ('AU', 'AUSTRALIA') then 'AUSTRALIA'
            when trim(upper(country)) in ('DE', 'GERMANY') then 'GERMANY'
            when trim(upper(country)) in ('FR', 'FRANCE') then 'FRANCE'
            when trim(upper(country)) = '' or country is null then 'UNKNOWN'
            else trim(upper(country))
        end as country_standardized,
        
        -- Shape cleaning
        case 
            when trim(upper(shape)) = '' or shape is null then 'UNKNOWN'
            else trim(upper(shape))
        end as shape_clean,
        
        -- Duration cleaning and categorization
        case 
            when duration <= 0 or duration is null then null
            else duration 
        end as duration_seconds_clean,
        
        case 
            when duration <= 0 or duration is null then 'UNKNOWN'
            when duration <= 60 then 'VERY SHORT (≤1 min)'
            when duration <= 300 then 'SHORT (1-5 min)'
            when duration <= 1800 then 'MEDIUM (5-30 min)'
            when duration <= 3600 then 'LONG (30min-1hr)'
            else 'VERY LONG (>1hr)'
        end as duration_category,
        
        duration_hours,
        duration_minutes,
        
        -- Coordinates cleaning
        case 
            when latitude between -90 and 90 then latitude
            else null 
        end as latitude_clean,
        
        case 
            when longitude between -180 and 180 then longitude
            else null 
        end as longitude_clean,
        
        -- Comments cleaning
        case 
            when trim(comments) = '' or comments is null then null
            else trim(comments)
        end as comments_clean,
        
        -- Add data quality flags
        case 
            when try_to_timestamp(datetime) is null then false
            else true 
        end as has_valid_datetime,
        
        case 
            when latitude between -90 and 90 and longitude between -180 and 180 then true
            else false 
        end as has_valid_coordinates,
        
        case 
            when trim(country) != '' and country is not null then true
            else false 
        end as has_country_data

    from ufo_sightings
),

final as (
    select 
        *,
        -- Create a unique identifier for each sighting
        hash(concat(
            coalesce(sighting_datetime::string, ''),
            '|',
            coalesce(city_clean, ''),
            '|',
            coalesce(state_clean, ''),
            '|',
            coalesce(country_clean, ''),
            '|',
            coalesce(shape_clean, '')
        )) as sighting_id
    
    from cleaned_sightings
    where sighting_datetime is not null  -- Filter out records without valid dates
)

select * from final
