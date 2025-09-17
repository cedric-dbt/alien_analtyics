{{
  config(
    materialized='table',
    description='UFO sightings aggregated by country and time periods for dashboard consumption'
  )
}}

with enriched_sightings as (
    select * from {{ ref('int_ufo_location_enriched') }}
),

monthly_country_aggregates as (
    select
        country_standardized as country,
        region,
        sighting_year as year,
        sighting_month as month,
        date_trunc('month', sighting_date) as month_date,
        
        -- Core metrics
        count(*) as total_sightings,
        count(distinct city_clean) as unique_cities,
        count(distinct state_clean) as unique_states,
        count(case when has_valid_coordinates then 1 end) as sightings_with_coordinates,
        
        -- Duration metrics
        avg(duration_seconds_clean) as avg_duration_seconds,
        median(duration_seconds_clean) as median_duration_seconds,
        max(duration_seconds_clean) as max_duration_seconds,
        
        -- Shape distribution (top shapes)
        count(case when shape_clean = 'LIGHT' then 1 end) as light_sightings,
        count(case when shape_clean = 'TRIANGLE' then 1 end) as triangle_sightings,
        count(case when shape_clean = 'CIRCLE' then 1 end) as circle_sightings,
        count(case when shape_clean = 'DISK' then 1 end) as disk_sightings,
        count(case when shape_clean = 'SPHERE' then 1 end) as sphere_sightings,
        count(case when shape_clean = 'OTHER' then 1 end) as other_shape_sightings,
        
        -- Duration categories
        count(case when duration_category = 'VERY SHORT (≤1 min)' then 1 end) as very_short_duration,
        count(case when duration_category = 'SHORT (1-5 min)' then 1 end) as short_duration,
        count(case when duration_category = 'MEDIUM (5-30 min)' then 1 end) as medium_duration,
        count(case when duration_category = 'LONG (30min-1hr)' then 1 end) as long_duration,
        count(case when duration_category = 'VERY LONG (>1hr)' then 1 end) as very_long_duration,
        
        -- Data quality metrics
        round(count(case when has_valid_coordinates then 1 end) * 100.0 / count(*), 2) as pct_with_coordinates,
        round(count(case when comments_clean is not null then 1 end) * 100.0 / count(*), 2) as pct_with_comments,
        
        -- Geographic center (for mapping)
        avg(latitude_clean) as avg_latitude,
        avg(longitude_clean) as avg_longitude
        
    from enriched_sightings
    where has_valid_datetime = true
    group by 
        country_standardized, 
        region,
        sighting_year, 
        sighting_month,
        date_trunc('month', sighting_date)
),

yearly_country_aggregates as (
    select
        country,
        region,
        year,
        null as month,
        date_trunc('year', date(year || '-01-01')) as year_date,
        
        sum(total_sightings) as total_sightings,
        sum(unique_cities) as unique_cities,
        sum(unique_states) as unique_states,
        sum(sightings_with_coordinates) as sightings_with_coordinates,
        
        avg(avg_duration_seconds) as avg_duration_seconds,
        avg(median_duration_seconds) as median_duration_seconds,
        max(max_duration_seconds) as max_duration_seconds,
        
        sum(light_sightings) as light_sightings,
        sum(triangle_sightings) as triangle_sightings,
        sum(circle_sightings) as circle_sightings,
        sum(disk_sightings) as disk_sightings,
        sum(sphere_sightings) as sphere_sightings,
        sum(other_shape_sightings) as other_shape_sightings,
        
        sum(very_short_duration) as very_short_duration,
        sum(short_duration) as short_duration,
        sum(medium_duration) as medium_duration,
        sum(long_duration) as long_duration,
        sum(very_long_duration) as very_long_duration,
        
        round(sum(sightings_with_coordinates) * 100.0 / sum(total_sightings), 2) as pct_with_coordinates,
        round(sum(case when pct_with_comments > 0 then total_sightings * pct_with_comments / 100.0 else 0 end) * 100.0 / sum(total_sightings), 2) as pct_with_comments,
        
        avg(avg_latitude) as avg_latitude,
        avg(avg_longitude) as avg_longitude
        
    from monthly_country_aggregates
    group by country, region, year
),

combined_aggregates as (
    -- Monthly data
    select
        country,
        region,
        year,
        month,
        'monthly' as time_grain,
        month_date as period_date,
        total_sightings,
        unique_cities,
        unique_states,
        sightings_with_coordinates,
        avg_duration_seconds,
        median_duration_seconds,
        max_duration_seconds,
        light_sightings,
        triangle_sightings,
        circle_sightings,
        disk_sightings,
        sphere_sightings,
        other_shape_sightings,
        very_short_duration,
        short_duration,
        medium_duration,
        long_duration,
        very_long_duration,
        pct_with_coordinates,
        pct_with_comments,
        avg_latitude,
        avg_longitude
    from monthly_country_aggregates
    
    union all
    
    -- Yearly data
    select
        country,
        region,
        year,
        month,
        'yearly' as time_grain,
        year_date as period_date,
        total_sightings,
        unique_cities,
        unique_states,
        sightings_with_coordinates,
        avg_duration_seconds,
        median_duration_seconds,
        max_duration_seconds,
        light_sightings,
        triangle_sightings,
        circle_sightings,
        disk_sightings,
        sphere_sightings,
        other_shape_sightings,
        very_short_duration,
        short_duration,
        medium_duration,
        long_duration,
        very_long_duration,
        pct_with_coordinates,
        pct_with_comments,
        avg_latitude,
        avg_longitude
    from yearly_country_aggregates
)

select * from combined_aggregates
order by country, time_grain, period_date
