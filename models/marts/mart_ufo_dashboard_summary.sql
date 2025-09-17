{{
  config(
    materialized='table',
    description='High-level UFO sightings summary metrics for dashboard KPIs'
  )
}}

with enriched_sightings as (
    select * from {{ ref('int_ufo_location_enriched') }}
),

overall_summary as (
    select
        'overall' as metric_type,
        'all_time' as time_period,
        null as country,
        null as region,
        
        -- Core counts
        count(*) as total_sightings,
        count(distinct country_standardized) as unique_countries,
        count(distinct concat(country_standardized, '|', state_clean)) as unique_states,
        count(distinct concat(country_standardized, '|', state_clean, '|', city_clean)) as unique_cities,
        
        -- Time range
        min(sighting_date) as earliest_sighting,
        max(sighting_date) as latest_sighting,
        datediff('year', min(sighting_date), max(sighting_date)) as years_of_data,
        
        -- Shape distribution
        count(distinct shape_clean) as unique_shapes,
        mode(shape_clean) as most_common_shape,
        
        -- Duration insights
        avg(duration_seconds_clean) as avg_duration_seconds,
        median(duration_seconds_clean) as median_duration_seconds,
        max(duration_seconds_clean) as max_duration_seconds,
        
        -- Data quality
        count(case when has_valid_coordinates then 1 end) as sightings_with_coordinates,
        round(count(case when has_valid_coordinates then 1 end) * 100.0 / nullif(count(*), 0), 2) as pct_with_coordinates,
        count(case when comments_clean is not null then 1 end) as sightings_with_comments,
        round(count(case when comments_clean is not null then 1 end) * 100.0 / nullif(count(*), 0), 2) as pct_with_comments,
        
        -- Recent activity trends
        count(case when sighting_year >= year(current_date()) - 1 then 1 end) as sightings_last_year,
        count(case when sighting_year >= year(current_date()) - 5 then 1 end) as sightings_last_5_years,
        count(case when sighting_year >= year(current_date()) - 10 then 1 end) as sightings_last_10_years
        
    from enriched_sightings
    where has_valid_datetime = true
),

country_summary as (
    select
        'by_country' as metric_type,
        'all_time' as time_period,
        country_standardized as country,
        region,
        
        count(*) as total_sightings,
        1 as unique_countries,
        count(distinct state_clean) as unique_states,
        count(distinct city_clean) as unique_cities,
        
        min(sighting_date) as earliest_sighting,
        max(sighting_date) as latest_sighting,
        datediff('year', min(sighting_date), max(sighting_date)) as years_of_data,
        
        count(distinct shape_clean) as unique_shapes,
        mode(shape_clean) as most_common_shape,
        
        avg(duration_seconds_clean) as avg_duration_seconds,
        median(duration_seconds_clean) as median_duration_seconds,
        max(duration_seconds_clean) as max_duration_seconds,
        
        count(case when has_valid_coordinates then 1 end) as sightings_with_coordinates,
        round(count(case when has_valid_coordinates then 1 end) * 100.0 / nullif(count(*), 0), 2) as pct_with_coordinates,
        count(case when comments_clean is not null then 1 end) as sightings_with_comments,
        round(count(case when comments_clean is not null then 1 end) * 100.0 / nullif(count(*), 0), 2) as pct_with_comments,
        
        count(case when sighting_year >= year(current_date()) - 1 then 1 end) as sightings_last_year,
        count(case when sighting_year >= year(current_date()) - 5 then 1 end) as sightings_last_5_years,
        count(case when sighting_year >= year(current_date()) - 10 then 1 end) as sightings_last_10_years
        
    from enriched_sightings
    where has_valid_datetime = true
    group by country_standardized, region
),

recent_trends as (
    select
        'recent_trends' as metric_type,
        case 
            when sighting_year >= year(current_date()) - 1 then 'last_year'
            when sighting_year >= year(current_date()) - 5 then 'last_5_years'
            when sighting_year >= year(current_date()) - 10 then 'last_10_years'
            else 'older'
        end as time_period,
        null as country,
        null as region,
        
        count(*) as total_sightings,
        count(distinct country_standardized) as unique_countries,
        count(distinct concat(country_standardized, '|', state_clean)) as unique_states,
        count(distinct concat(country_standardized, '|', state_clean, '|', city_clean)) as unique_cities,
        
        min(sighting_date) as earliest_sighting,
        max(sighting_date) as latest_sighting,
        null as years_of_data,
        
        count(distinct shape_clean) as unique_shapes,
        mode(shape_clean) as most_common_shape,
        
        avg(duration_seconds_clean) as avg_duration_seconds,
        median(duration_seconds_clean) as median_duration_seconds,
        max(duration_seconds_clean) as max_duration_seconds,
        
        count(case when has_valid_coordinates then 1 end) as sightings_with_coordinates,
        round(count(case when has_valid_coordinates then 1 end) * 100.0 / nullif(count(*), 0), 2) as pct_with_coordinates,
        count(case when comments_clean is not null then 1 end) as sightings_with_comments,
        round(count(case when comments_clean is not null then 1 end) * 100.0 / nullif(count(*), 0), 2) as pct_with_comments,
        
        null as sightings_last_year,
        null as sightings_last_5_years,
        null as sightings_last_10_years
        
    from enriched_sightings
    where has_valid_datetime = true
    group by 
        case 
            when sighting_year >= year(current_date()) - 1 then 'last_year'
            when sighting_year >= year(current_date()) - 5 then 'last_5_years'
            when sighting_year >= year(current_date()) - 10 then 'last_10_years'
            else 'older'
        end
),

combined_summary as (
    select * from overall_summary
    union all
    select * from country_summary
    union all
    select * from recent_trends
)

select 
    metric_type,
    time_period,
    country,
    region,
    total_sightings,
    unique_countries,
    unique_states,
    unique_cities,
    earliest_sighting,
    latest_sighting,
    years_of_data,
    unique_shapes,
    most_common_shape,
    round(avg_duration_seconds, 2) as avg_duration_seconds,
    round(median_duration_seconds, 2) as median_duration_seconds,
    max_duration_seconds,
    sightings_with_coordinates,
    pct_with_coordinates,
    sightings_with_comments,
    pct_with_comments,
    sightings_last_year,
    sightings_last_5_years,
    sightings_last_10_years,
    
    -- Add calculated rates
    case 
        when years_of_data > 0 then round(total_sightings / nullif(years_of_data, 0), 1)
        else null 
    end as avg_sightings_per_year
    
from combined_summary
order by metric_type, time_period, total_sightings desc
