{{
  config(
    materialized='view'
  )
}}

with flights_base as (
    select * from {{ ref('stg_airlines_flights') }}
),

flight_volume_metrics as (
    select
        -- Time dimensions for aggregation
        departure_hour,
        departure_time_of_day,
        
        -- Route information
        departure_city,
        arrival_city,
        flight_route,
        
        -- Flight characteristics
        airline_name,
        airline_region,
        stop_category,
        flight_duration_category,
        
        -- Volume metrics
        count(*) as total_flights,
        count(distinct airline_name) as unique_airlines,
        count(distinct flight_route) as unique_routes,
        
        -- Duration analysis
        avg(flight_duration_hours) as avg_flight_duration,
        min(flight_duration_hours) as min_flight_duration,
        max(flight_duration_hours) as max_flight_duration,
        
        -- Pricing analysis (for context)
        avg(ticket_price) as avg_ticket_price,
        min(ticket_price) as min_ticket_price,
        max(ticket_price) as max_ticket_price,
        
        -- Stop analysis
        avg(number_of_stops) as avg_stops,
        sum(case when number_of_stops = 0 then 1 else 0 end) as nonstop_flights,
        sum(case when number_of_stops >= 1 then 1 else 0 end) as connecting_flights,
        
        -- Data quality metrics
        sum(case when has_valid_duration then 1 else 0 end) as flights_with_duration,
        sum(case when has_pricing_data then 1 else 0 end) as flights_with_pricing,
        sum(case when has_complete_route then 1 else 0 end) as flights_with_complete_route,
        
        -- Calculate percentages
        round(
            (sum(case when number_of_stops = 0 then 1 else 0 end) * 100.0 / count(*)), 2
        ) as nonstop_percentage,
        
        round(
            (sum(case when has_valid_duration then 1 else 0 end) * 100.0 / count(*)), 2
        ) as data_completeness_percentage
        
    from flights_base
    where has_complete_route = true  -- Focus on flights with complete route data
    group by 
        departure_hour,
        departure_time_of_day,
        departure_city,
        arrival_city,
        flight_route,
        airline_name,
        airline_region,
        stop_category,
        flight_duration_category
)

select * from flight_volume_metrics
