{{
  config(
    materialized='table'
  )
}}

with ufo_sightings as (
    select * from {{ ref('int_ufo_location_enriched') }}
),

airplane_crashes as (
    select * from {{ ref('stg_airplane_crashes') }}
),

-- Find UFO sightings that occurred within spatial and temporal proximity of aircraft crashes
ufo_crash_proximity as (
    select 
        u.sighting_id,
        u.sighting_datetime,
        u.sighting_date,
        u.city_clean as ufo_city,
        u.state_clean as ufo_state,
        u.country_standardized as ufo_country,
        u.latitude_clean as ufo_latitude,
        u.longitude_clean as ufo_longitude,
        u.shape_clean as ufo_shape,
        u.duration_seconds_clean as ufo_duration,
        u.comments_clean as ufo_comments,
        
        c.crash_date,
        c.crash_location,
        c.crash_country,
        c.airline_operator,
        c.aircraft_type,
        c.aircraft_manufacturer,
        c.operator_type,
        c.total_fatalities,
        c.casualty_severity,
        c.crash_summary,
        
        -- Calculate time difference (UFO sighting vs crash)
        datediff('day', c.crash_date, u.sighting_date) as days_after_crash,
        abs(datediff('day', c.crash_date, u.sighting_date)) as days_difference,
        
        -- Spatial proximity indicators (rough distance calculation)
        case 
            when u.country_standardized = c.crash_country then true
            else false 
        end as same_country,
        
        case 
            when u.country_standardized = c.crash_country 
                 and u.state_clean = split_part(c.crash_location, ',', 1) then true
            else false 
        end as same_region,
        
        -- Correlation strength scoring
        case 
            when abs(datediff('day', c.crash_date, u.sighting_date)) <= 1 
                 and u.country_standardized = c.crash_country then 'VERY_HIGH'
            when abs(datediff('day', c.crash_date, u.sighting_date)) <= 7 
                 and u.country_standardized = c.crash_country then 'HIGH'
            when abs(datediff('day', c.crash_date, u.sighting_date)) <= 30 
                 and u.country_standardized = c.crash_country then 'MODERATE'
            when abs(datediff('day', c.crash_date, u.sighting_date)) <= 90 
                 and u.country_standardized = c.crash_country then 'LOW'
            else 'VERY_LOW'
        end as correlation_strength,
        
        -- Aircraft-like characteristics in UFO reports
        case 
            when upper(u.shape_clean) in ('DISK', 'CIRCLE', 'SPHERE') 
                 and u.duration_seconds_clean between 30 and 300 then true
            when upper(u.shape_clean) in ('LIGHT', 'FIREBALL') 
                 and u.duration_seconds_clean between 10 and 120 then true
            when upper(u.comments_clean) like '%CRASH%' 
                 or upper(u.comments_clean) like '%PLANE%' 
                 or upper(u.comments_clean) like '%AIRCRAFT%' then true
            else false 
        end as has_aircraft_characteristics,
        
        -- Media coverage potential (high-casualty crashes get more attention)
        case 
            when c.total_fatalities > 100 then 'HIGH_MEDIA'
            when c.total_fatalities > 20 then 'MODERATE_MEDIA'
            when c.total_fatalities > 0 then 'LOW_MEDIA'
            else 'MINIMAL_MEDIA'
        end as media_attention_level
        
    from ufo_sightings u
    cross join airplane_crashes c
    where 
        -- Only consider crashes and sightings within reasonable time window
        abs(datediff('day', c.crash_date, u.sighting_date)) <= 365
        -- Only consider same country or nearby regions
        and (
            u.country_standardized = c.crash_country
            or (u.country_standardized = 'UNITED STATES' and c.crash_country like '%USA%')
            or (u.country_standardized = 'CANADA' and c.crash_country like '%CANADA%')
        )
        -- Exclude very old crashes (before modern UFO reporting)
        and c.crash_year >= 1947  -- Start of modern UFO era
),

-- Add closest crash details using window functions
ufo_with_closest_crash as (
    select 
        *,
        row_number() over (partition by sighting_id order by days_difference) as crash_rank
    from ufo_crash_proximity
),

closest_crash_details as (
    select 
        sighting_id,
        crash_date as closest_crash_date,
        crash_location as closest_crash_location,
        aircraft_type as closest_aircraft_type,
        total_fatalities as closest_crash_fatalities
    from ufo_with_closest_crash
    where crash_rank = 1
),

-- Aggregate statistics for each UFO sighting
ufo_correlation_summary as (
    select 
        u.sighting_id,
        u.sighting_datetime,
        u.sighting_date,
        u.ufo_city,
        u.ufo_state, 
        u.ufo_country,
        u.ufo_latitude,
        u.ufo_longitude,
        u.ufo_shape,
        u.ufo_duration,
        u.ufo_comments,
        
        -- Count nearby crashes
        count(*) as nearby_crashes_count,
        count(case when u.days_difference <= 7 then 1 end) as crashes_within_week,
        count(case when u.days_difference <= 30 then 1 end) as crashes_within_month,
        count(case when u.same_country then 1 end) as crashes_same_country,
        
        -- Closest crash details
        min(u.days_difference) as closest_crash_days,
        max(c.closest_crash_date) as closest_crash_date,
        max(c.closest_crash_location) as closest_crash_location,
        max(c.closest_aircraft_type) as closest_aircraft_type,
        max(c.closest_crash_fatalities) as closest_crash_fatalities,
        
        -- Correlation metrics
        max(case 
            when u.correlation_strength = 'VERY_HIGH' then 5
            when u.correlation_strength = 'HIGH' then 4
            when u.correlation_strength = 'MODERATE' then 3
            when u.correlation_strength = 'LOW' then 2
            else 1
        end) as max_correlation_score,
        
        max(case when u.has_aircraft_characteristics then 1 else 0 end) = 1 as has_aircraft_like_features,
        max(u.media_attention_level) as max_media_attention,
        
        -- Calculate overall suspicion score
        case 
            when count(case when u.days_difference <= 7 then 1 end) > 0 
                 and max(case when u.has_aircraft_characteristics then 1 else 0 end) = 1 then 'HIGH_SUSPICION'
            when count(case when u.days_difference <= 30 then 1 end) > 0 
                 and count(case when u.same_country then 1 end) > 0 then 'MODERATE_SUSPICION'
            when count(case when u.days_difference <= 90 then 1 end) > 0 then 'LOW_SUSPICION'
            else 'MINIMAL_SUSPICION'
        end as false_positive_likelihood
        
    from ufo_crash_proximity u
    left join closest_crash_details c on u.sighting_id = c.sighting_id
    group by 
        u.sighting_id, u.sighting_datetime, u.sighting_date, u.ufo_city, u.ufo_state, 
        u.ufo_country, u.ufo_latitude, u.ufo_longitude, u.ufo_shape, u.ufo_duration, u.ufo_comments
)

select * from ufo_correlation_summary
