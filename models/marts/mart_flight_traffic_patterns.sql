{{
  config(
    materialized='table'
  )
}}

with flight_base as (
    select * from {{ ref('stg_airlines_flights') }}
    where has_complete_route = true
),

-- Hourly flight patterns
hourly_patterns as (
    select
        departure_hour,
        departure_time_of_day,
        
        count(*) as flights_in_hour,
        count(distinct airline_name) as airlines_active_in_hour,
        count(distinct flight_route) as routes_active_in_hour,
        
        -- Flight characteristics by hour
        avg(flight_duration_hours) as avg_duration_in_hour,
        sum(case when number_of_stops = 0 then 1 else 0 end) as nonstop_flights_in_hour,
        sum(case when airline_region = 'US_MAJOR' then 1 else 0 end) as us_major_flights_in_hour,
        sum(case when airline_region = 'EUROPEAN' then 1 else 0 end) as european_flights_in_hour,
        sum(case when airline_region = 'ASIAN' then 1 else 0 end) as asian_flights_in_hour,
        
        -- Duration categories by hour
        sum(case when flight_duration_category = 'SHORT_HAUL' then 1 else 0 end) as short_haul_in_hour,
        sum(case when flight_duration_category = 'MEDIUM_HAUL' then 1 else 0 end) as medium_haul_in_hour,
        sum(case when flight_duration_category = 'LONG_HAUL' then 1 else 0 end) as long_haul_in_hour,
        sum(case when flight_duration_category = 'ULTRA_LONG_HAUL' then 1 else 0 end) as ultra_long_haul_in_hour
        
    from flight_base
    where departure_hour is not null
    group by departure_hour, departure_time_of_day
),

-- Airline-specific patterns for crash correlation
airline_patterns as (
    select
        airline_name,
        airline_region,
        
        count(*) as total_airline_flights,
        count(distinct flight_route) as routes_served,
        count(distinct departure_city) as cities_served_from,
        count(distinct arrival_city) as cities_served_to,
        
        -- Operational characteristics
        avg(flight_duration_hours) as avg_airline_duration,
        avg(number_of_stops) as avg_airline_stops,
        
        -- Time distribution for this airline
        sum(case when departure_time_of_day = 'MORNING' then 1 else 0 end) as airline_morning_flights,
        sum(case when departure_time_of_day = 'AFTERNOON' then 1 else 0 end) as airline_afternoon_flights,
        sum(case when departure_time_of_day = 'EVENING' then 1 else 0 end) as airline_evening_flights,
        sum(case when departure_time_of_day = 'NIGHT' then 1 else 0 end) as airline_night_flights,
        
        -- Flight types
        sum(case when flight_duration_category = 'SHORT_HAUL' then 1 else 0 end) as airline_short_haul,
        sum(case when flight_duration_category = 'MEDIUM_HAUL' then 1 else 0 end) as airline_medium_haul,
        sum(case when flight_duration_category = 'LONG_HAUL' then 1 else 0 end) as airline_long_haul,
        sum(case when flight_duration_category = 'ULTRA_LONG_HAUL' then 1 else 0 end) as airline_ultra_long_haul,
        
        -- Operational complexity
        round((sum(case when number_of_stops = 0 then 1 else 0 end) * 100.0 / count(*)), 2) as airline_nonstop_percentage,
        
        -- Calculate airline size category
        case 
            when count(*) >= 1000 then 'MAJOR_AIRLINE'
            when count(*) >= 500 then 'LARGE_AIRLINE'
            when count(*) >= 100 then 'MEDIUM_AIRLINE'
            when count(*) >= 50 then 'SMALL_AIRLINE'
            else 'MICRO_AIRLINE'
        end as airline_size_category
        
    from flight_base
    group by airline_name, airline_region
),

-- Overall traffic summary for context
traffic_summary as (
    select
        count(*) as total_flights_in_dataset,
        count(distinct airline_name) as total_airlines,
        count(distinct flight_route) as total_routes,
        count(distinct departure_city) as total_departure_cities,
        count(distinct arrival_city) as total_arrival_cities,
        
        avg(flight_duration_hours) as overall_avg_duration,
        avg(number_of_stops) as overall_avg_stops,
        
        -- Overall time distribution
        round((sum(case when departure_time_of_day = 'MORNING' then 1 else 0 end) * 100.0 / count(*)), 2) as morning_percentage,
        round((sum(case when departure_time_of_day = 'AFTERNOON' then 1 else 0 end) * 100.0 / count(*)), 2) as afternoon_percentage,
        round((sum(case when departure_time_of_day = 'EVENING' then 1 else 0 end) * 100.0 / count(*)), 2) as evening_percentage,
        round((sum(case when departure_time_of_day = 'NIGHT' then 1 else 0 end) * 100.0 / count(*)), 2) as night_percentage,
        
        -- Overall flight type distribution
        round((sum(case when flight_duration_category = 'SHORT_HAUL' then 1 else 0 end) * 100.0 / count(*)), 2) as short_haul_percentage,
        round((sum(case when flight_duration_category = 'MEDIUM_HAUL' then 1 else 0 end) * 100.0 / count(*)), 2) as medium_haul_percentage,
        round((sum(case when flight_duration_category = 'LONG_HAUL' then 1 else 0 end) * 100.0 / count(*)), 2) as long_haul_percentage,
        round((sum(case when flight_duration_category = 'ULTRA_LONG_HAUL' then 1 else 0 end) * 100.0 / count(*)), 2) as ultra_long_haul_percentage
        
    from flight_base
),

-- Combine all patterns with context
final_traffic_analysis as (
    select
        'HOURLY_PATTERN' as analysis_type,
        cast(departure_hour as varchar) as category_name,
        departure_time_of_day as subcategory,
        flights_in_hour as flight_count,
        airlines_active_in_hour as airline_count,
        routes_active_in_hour as route_count,
        avg_duration_in_hour as avg_duration,
        nonstop_flights_in_hour as nonstop_count,
        null as complexity_indicator,
        
        -- Calculate percentage of total traffic
        round((flights_in_hour * 100.0 / (select total_flights_in_dataset from traffic_summary)), 2) as percentage_of_total_traffic
        
    from hourly_patterns
    
    union all
    
    select
        'AIRLINE_PATTERN' as analysis_type,
        airline_name as category_name,
        airline_region as subcategory,
        total_airline_flights as flight_count,
        null as airline_count,
        routes_served as route_count,
        avg_airline_duration as avg_duration,
        null as nonstop_count,
        airline_nonstop_percentage as complexity_indicator,
        
        -- Calculate percentage of total traffic
        round((total_airline_flights * 100.0 / (select total_flights_in_dataset from traffic_summary)), 2) as percentage_of_total_traffic
        
    from airline_patterns
)

select * from final_traffic_analysis
order by analysis_type, flight_count desc
