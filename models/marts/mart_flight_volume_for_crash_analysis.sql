{{
  config(
    materialized='table'
  )
}}

with flight_volumes as (
    select * from {{ ref('int_flight_volume_analysis') }}
),

-- Aggregate by route for crash correlation
route_volumes as (
    select
        departure_city,
        arrival_city,
        flight_route,
        
        -- Total flight volume metrics
        sum(total_flights) as total_route_flights,
        sum(unique_airlines) as airlines_serving_route,
        
        -- Time distribution
        sum(case when departure_time_of_day = 'MORNING' then total_flights else 0 end) as morning_flights,
        sum(case when departure_time_of_day = 'AFTERNOON' then total_flights else 0 end) as afternoon_flights,
        sum(case when departure_time_of_day = 'EVENING' then total_flights else 0 end) as evening_flights,
        sum(case when departure_time_of_day = 'NIGHT' then total_flights else 0 end) as night_flights,
        
        -- Flight characteristics
        sum(nonstop_flights) as total_nonstop_flights,
        sum(connecting_flights) as total_connecting_flights,
        
        -- Average metrics weighted by flight volume
        sum(avg_flight_duration * total_flights) / sum(total_flights) as weighted_avg_duration,
        sum(avg_ticket_price * total_flights) / nullif(sum(total_flights), 0) as weighted_avg_price,
        
        -- Route complexity metrics
        sum(case when stop_category = 'NONSTOP' then total_flights else 0 end) as route_nonstop_volume,
        sum(case when stop_category = 'ONE_STOP' then total_flights else 0 end) as route_one_stop_volume,
        sum(case when stop_category = 'MULTIPLE_STOPS' then total_flights else 0 end) as route_multi_stop_volume,
        
        -- Duration categories
        sum(case when flight_duration_category = 'SHORT_HAUL' then total_flights else 0 end) as short_haul_flights,
        sum(case when flight_duration_category = 'MEDIUM_HAUL' then total_flights else 0 end) as medium_haul_flights,
        sum(case when flight_duration_category = 'LONG_HAUL' then total_flights else 0 end) as long_haul_flights,
        sum(case when flight_duration_category = 'ULTRA_LONG_HAUL' then total_flights else 0 end) as ultra_long_haul_flights
        
    from flight_volumes
    group by departure_city, arrival_city, flight_route
),

-- City-level aggregations for broader analysis
city_volumes as (
    select
        departure_city,
        sum(total_route_flights) as total_departing_flights,
        count(distinct arrival_city) as destinations_served,
        avg(weighted_avg_duration) as avg_outbound_duration,
        sum(morning_flights) as city_morning_departures,
        sum(afternoon_flights) as city_afternoon_departures,
        sum(evening_flights) as city_evening_departures,
        sum(night_flights) as city_night_departures
    from route_volumes
    group by departure_city
),

arrival_city_volumes as (
    select
        arrival_city,
        sum(total_route_flights) as total_arriving_flights,
        count(distinct departure_city) as origins_served,
        avg(weighted_avg_duration) as avg_inbound_duration
    from route_volumes
    group by arrival_city
),

-- Final enriched route data with city context
final_route_analysis as (
    select
        rv.*,
        
        -- Departure city context
        cv.total_departing_flights as departure_city_total_flights,
        cv.destinations_served as departure_city_destinations,
        
        -- Arrival city context  
        acv.total_arriving_flights as arrival_city_total_flights,
        acv.origins_served as arrival_city_origins,
        
        -- Route importance metrics
        round(
            (rv.total_route_flights * 100.0 / cv.total_departing_flights), 2
        ) as route_share_of_departure_city,
        
        round(
            (rv.total_route_flights * 100.0 / acv.total_arriving_flights), 2
        ) as route_share_of_arrival_city,
        
        -- Flight density categories for crash risk analysis
        case 
            when rv.total_route_flights >= 1000 then 'VERY_HIGH_VOLUME'
            when rv.total_route_flights >= 500 then 'HIGH_VOLUME'
            when rv.total_route_flights >= 100 then 'MEDIUM_VOLUME'
            when rv.total_route_flights >= 50 then 'LOW_VOLUME'
            else 'VERY_LOW_VOLUME'
        end as route_volume_category,
        
        -- Time distribution analysis
        round((rv.morning_flights * 100.0 / rv.total_route_flights), 2) as morning_flight_percentage,
        round((rv.afternoon_flights * 100.0 / rv.total_route_flights), 2) as afternoon_flight_percentage,
        round((rv.evening_flights * 100.0 / rv.total_route_flights), 2) as evening_flight_percentage,
        round((rv.night_flights * 100.0 / rv.total_route_flights), 2) as night_flight_percentage,
        
        -- Complexity indicators
        case 
            when rv.route_nonstop_volume = rv.total_route_flights then 'NONSTOP_ONLY'
            when rv.route_nonstop_volume > (rv.total_route_flights * 0.8) then 'MOSTLY_NONSTOP'
            when rv.route_multi_stop_volume > (rv.total_route_flights * 0.5) then 'COMPLEX_ROUTING'
            else 'MIXED_ROUTING'
        end as route_complexity_category
        
    from route_volumes rv
    left join city_volumes cv on rv.departure_city = cv.departure_city
    left join arrival_city_volumes acv on rv.arrival_city = acv.arrival_city
)

select * from final_route_analysis
order by total_route_flights desc
