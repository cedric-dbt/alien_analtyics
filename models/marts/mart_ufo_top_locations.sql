{{
  config(
    materialized='table',
    description='Top UFO sighting locations with detailed metrics for dashboard consumption'
  )
}}

with enriched_sightings as (
    select * from {{ ref('int_ufo_location_enriched') }}
),

country_rankings as (
    select
        country_standardized as country,
        region,
        count(*) as total_sightings,
        count(distinct state_clean) as unique_states,
        count(distinct city_clean) as unique_cities,
        min(sighting_date) as first_sighting_date,
        max(sighting_date) as last_sighting_date,
        datediff('day', min(sighting_date), max(sighting_date)) as days_of_activity,
        
        -- Duration metrics
        avg(duration_seconds_clean) as avg_duration_seconds,
        median(duration_seconds_clean) as median_duration_seconds,
        
        -- Most common shape per country
        mode(shape_clean) as most_common_shape,
        
        -- Geographic center
        avg(latitude_clean) as center_latitude,
        avg(longitude_clean) as center_longitude,
        
        -- Data quality
        count(case when has_valid_coordinates then 1 end) as sightings_with_coordinates,
        round(count(case when has_valid_coordinates then 1 end) * 100.0 / count(*), 2) as pct_with_coordinates,
        
        -- Recent activity (last 5 years)
        count(case when sighting_year >= year(current_date()) - 5 then 1 end) as recent_sightings,
        
        -- Activity level
        case 
            when count(*) >= 1000 then 'VERY HIGH'
            when count(*) >= 500 then 'HIGH'
            when count(*) >= 100 then 'MEDIUM'
            when count(*) >= 20 then 'LOW'
            else 'MINIMAL'
        end as activity_level
        
    from enriched_sightings
    where has_valid_datetime = true
    group by country_standardized, region
),

state_rankings as (
    select
        country_standardized as country,
        region,
        state_clean as state,
        count(*) as total_sightings,
        count(distinct city_clean) as unique_cities,
        min(sighting_date) as first_sighting_date,
        max(sighting_date) as last_sighting_date,
        
        -- Duration metrics
        avg(duration_seconds_clean) as avg_duration_seconds,
        median(duration_seconds_clean) as median_duration_seconds,
        
        -- Most common shape per state
        mode(shape_clean) as most_common_shape,
        
        -- Geographic center
        avg(latitude_clean) as center_latitude,
        avg(longitude_clean) as center_longitude,
        
        -- Data quality
        count(case when has_valid_coordinates then 1 end) as sightings_with_coordinates,
        round(count(case when has_valid_coordinates then 1 end) * 100.0 / count(*), 2) as pct_with_coordinates,
        
        -- Recent activity
        count(case when sighting_year >= year(current_date()) - 5 then 1 end) as recent_sightings,
        
        -- Ranking within country
        row_number() over (partition by country_standardized order by count(*) desc) as country_rank
        
    from enriched_sightings
    where has_valid_datetime = true
    group by country_standardized, region, state_clean
),

city_rankings as (
    select
        country_standardized as country,
        region,
        state_clean as state,
        city_clean as city,
        count(*) as total_sightings,
        min(sighting_date) as first_sighting_date,
        max(sighting_date) as last_sighting_date,
        
        -- Duration metrics
        avg(duration_seconds_clean) as avg_duration_seconds,
        median(duration_seconds_clean) as median_duration_seconds,
        
        -- Most common shape per city
        mode(shape_clean) as most_common_shape,
        
        -- Geographic center
        avg(latitude_clean) as center_latitude,
        avg(longitude_clean) as center_longitude,
        
        -- Data quality
        count(case when has_valid_coordinates then 1 end) as sightings_with_coordinates,
        round(count(case when has_valid_coordinates then 1 end) * 100.0 / count(*), 2) as pct_with_coordinates,
        
        -- Recent activity
        count(case when sighting_year >= year(current_date()) - 5 then 1 end) as recent_sightings,
        
        -- Rankings
        row_number() over (order by count(*) desc) as global_rank,
        row_number() over (partition by country_standardized order by count(*) desc) as country_rank,
        row_number() over (partition by country_standardized, state_clean order by count(*) desc) as state_rank
        
    from enriched_sightings
    where has_valid_datetime = true
    group by country_standardized, region, state_clean, city_clean
),

combined_rankings as (
    -- Country level
    select
        'country' as location_type,
        country,
        region,
        null as state,
        null as city,
        total_sightings,
        unique_states as sub_locations,
        unique_cities,
        first_sighting_date,
        last_sighting_date,
        days_of_activity,
        avg_duration_seconds,
        median_duration_seconds,
        most_common_shape,
        center_latitude,
        center_longitude,
        sightings_with_coordinates,
        pct_with_coordinates,
        recent_sightings,
        activity_level,
        row_number() over (order by total_sightings desc) as global_rank,
        null as country_rank,
        null as state_rank
    from country_rankings
    
    union all
    
    -- State level (top 50 globally)
    select
        'state' as location_type,
        country,
        region,
        state,
        null as city,
        total_sightings,
        null as sub_locations,
        unique_cities,
        first_sighting_date,
        last_sighting_date,
        null as days_of_activity,
        avg_duration_seconds,
        median_duration_seconds,
        most_common_shape,
        center_latitude,
        center_longitude,
        sightings_with_coordinates,
        pct_with_coordinates,
        recent_sightings,
        null as activity_level,
        global_state_rank as global_rank,
        country_rank,
        null as state_rank
    from (
        select *,
               row_number() over (order by total_sightings desc) as global_state_rank
        from state_rankings
    ) ranked_states
    where global_state_rank <= 50
    
    union all
    
    -- City level (top 100 globally)
    select
        'city' as location_type,
        country,
        region,
        state,
        city,
        total_sightings,
        null as sub_locations,
        null as unique_cities,
        first_sighting_date,
        last_sighting_date,
        null as days_of_activity,
        avg_duration_seconds,
        median_duration_seconds,
        most_common_shape,
        center_latitude,
        center_longitude,
        sightings_with_coordinates,
        pct_with_coordinates,
        recent_sightings,
        null as activity_level,
        global_rank,
        country_rank,
        state_rank
    from city_rankings
    where global_rank <= 100
)

select * from combined_rankings
order by location_type, global_rank
