{{
  config(
    materialized='table'
  )
}}

with ufo_sightings as (
    -- Use the parsed staging model so we benefit from parsing/normalization already performed
    select * from {{ ref('stg_ufo') }}
),

cleaned_sightings as (
    select
        -- Reuse the parsed event_datetime from staging
        event_datetime as sighting_datetime,
        event_date as sighting_date,
        year(event_date) as sighting_year,
        month(event_date) as sighting_month,
        dayofweek(event_date) as sighting_day_of_week,
        quarter(event_date) as sighting_quarter,

        -- Location cleaning (preserve values even when coordinates are null)
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
        duration_seconds as duration_seconds_clean,
        case 
            when duration_seconds is null then 'UNKNOWN'
            when duration_seconds <= 60 then 'VERY SHORT (≤1 min)'
            when duration_seconds <= 300 then 'SHORT (1-5 min)'
            when duration_seconds <= 1800 then 'MEDIUM (5-30 min)'
            when duration_seconds <= 3600 then 'LONG (30min-1hr)'
            else 'VERY LONG (>1hr)'
        end as duration_category,

        duration_hours_min as duration_hours,
        null as duration_minutes,

        -- Coordinates cleaned by staging: latitude and longitude may be null
        latitude as latitude_clean,
        longitude as longitude_clean,

        -- Comments cleaning
        case 
            when trim(comments) = '' or comments is null then null
            else trim(comments)
        end as comments_clean,

        -- Data quality flags
        case when event_datetime is not null then true else false end as has_valid_datetime,
        case when latitude is not null and longitude is not null then true else false end as has_valid_coordinates,
        case when trim(country) != '' and country is not null then true else false end as has_country_data,

        -- expose geo_day_key from staging (if present)
        geo_day_key

    from ufo_sightings
),

final as (
    select 
        *,
        -- Create a unique identifier for each sighting: md5 of identifying fields + a row_number to guarantee uniqueness
        lower(md5(
            coalesce(to_varchar(sighting_datetime), '' ) || '|' ||
            coalesce(city_clean, '') || '|' ||
            coalesce(state_clean, '') || '|' ||
            coalesce(country_clean, '') || '|' ||
            coalesce(shape_clean, '')
        )) || '_' || lpad(cast(row_number() over (order by sighting_datetime, country_clean, state_clean, city_clean) as varchar), 6, '0') as sighting_id
    from cleaned_sightings
    where sighting_datetime is not null
)

select * from final
