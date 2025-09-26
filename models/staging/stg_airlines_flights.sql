{{
  config(
    materialized='view'
  )
}}

with flights_raw as (
    select * from {{ source('ufo_raw', 'airlines_flights_data') }}
),

flights_with_stops_parsed as (
    select *,
        -- Parse stops field that contains text values like 'zero', 'one', etc.
        case 
            when upper(trim(stops)) = 'ZERO' or upper(trim(stops)) = '0' then 0
            when upper(trim(stops)) = 'ONE' or upper(trim(stops)) = '1' then 1
            when upper(trim(stops)) = 'TWO' or upper(trim(stops)) = '2' then 2
            when upper(trim(stops)) = 'THREE' or upper(trim(stops)) = '3' then 3
            when upper(trim(stops)) = 'FOUR' or upper(trim(stops)) = '4' then 4
            when upper(trim(stops)) = 'FIVE' or upper(trim(stops)) = '5' then 5
            when try_cast(stops as integer) is not null and try_cast(stops as integer) >= 0 then try_cast(stops as integer)
            else 0
        end as stops_parsed
    from flights_raw
    where airline is not null  -- Filter out completely empty records
),

flights_cleaned as (
    select
        -- Primary identifiers
        index as flight_record_id,
        trim(upper(coalesce(airline, 'UNKNOWN'))) as airline_name,
        trim(upper(coalesce(flight, ''))) as flight_number,
        
        -- Route information
        trim(upper(coalesce(source_city, 'UNKNOWN'))) as departure_city,
        trim(upper(coalesce(destination_city, 'UNKNOWN'))) as arrival_city,
        
        -- Create a standardized route field
        trim(upper(coalesce(source_city, 'UNKNOWN'))) || ' -> ' || 
        trim(upper(coalesce(destination_city, 'UNKNOWN'))) as flight_route,
        
        -- Time information (stored as strings, need parsing)
        departure_time as departure_time_raw,
        arrival_time as arrival_time_raw,
        
        -- Parse time strings to extract hour/minute if possible
        case 
            when departure_time like '%:%' then
                try_to_time(departure_time)
            else null
        end as departure_time_parsed,
        
        case 
            when arrival_time like '%:%' then
                try_to_time(arrival_time)
            else null
        end as arrival_time_parsed,
        
        -- Extract hour from departure time for analysis
        case 
            when departure_time like '%:%' then
                try_cast(split_part(departure_time, ':', 1) as integer)
            else null
        end as departure_hour,
        
        -- Stops and routing
        stops_parsed as number_of_stops,
        
        case 
            when stops_parsed = 0 then 'NONSTOP'
            when stops_parsed = 1 then 'ONE_STOP'
            when stops_parsed >= 2 then 'MULTIPLE_STOPS'
            else 'UNKNOWN'
        end as stop_category,
        
        -- Flight duration with validation
        case 
            when duration > 0 and duration <= 24 then duration
            else null
        end as flight_duration_hours,
        
        -- Duration categories for analysis
        case 
            when duration <= 2 then 'SHORT_HAUL'
            when duration <= 6 then 'MEDIUM_HAUL'
            when duration <= 12 then 'LONG_HAUL'
            when duration > 12 then 'ULTRA_LONG_HAUL'
            else 'UNKNOWN'
        end as flight_duration_category,
        
        -- Travel class standardization
        trim(upper(coalesce(class, 'UNKNOWN'))) as travel_class,
        
        case 
            when upper(class) like '%ECONOMY%' or upper(class) like '%COACH%' then 'ECONOMY'
            when upper(class) like '%BUSINESS%' then 'BUSINESS'
            when upper(class) like '%FIRST%' then 'FIRST'
            when upper(class) like '%PREMIUM%' then 'PREMIUM_ECONOMY'
            else 'UNKNOWN'
        end as travel_class_category,
        
        -- Booking and pricing information
        case 
            when days_left >= 0 then days_left
            else null
        end as booking_lead_days,
        
        case 
            when days_left <= 1 then 'LAST_MINUTE'
            when days_left <= 7 then 'SHORT_NOTICE'
            when days_left <= 30 then 'ADVANCE'
            when days_left > 30 then 'EARLY_BOOKING'
            else 'UNKNOWN'
        end as booking_timing_category,
        
        case 
            when price > 0 then price
            else null
        end as ticket_price,
        
        -- Price categories (rough estimation)
        case 
            when price <= 200 then 'BUDGET'
            when price <= 500 then 'MODERATE'
            when price <= 1000 then 'PREMIUM'
            when price > 1000 then 'LUXURY'
            else 'UNKNOWN'
        end as price_category,
        
        -- Time of day categories for departure analysis
        case 
            when try_cast(split_part(departure_time, ':', 1) as integer) between 5 and 11 then 'MORNING'
            when try_cast(split_part(departure_time, ':', 1) as integer) between 12 and 17 then 'AFTERNOON'
            when try_cast(split_part(departure_time, ':', 1) as integer) between 18 and 21 then 'EVENING'
            when try_cast(split_part(departure_time, ':', 1) as integer) between 22 and 23 
                 or try_cast(split_part(departure_time, ':', 1) as integer) between 0 and 4 then 'NIGHT'
            else 'UNKNOWN'
        end as departure_time_of_day,
        
        -- Airline categorization (basic)
        case 
            when upper(airline) like '%AMERICAN%' or upper(airline) like '%DELTA%' 
                 or upper(airline) like '%UNITED%' or upper(airline) like '%SOUTHWEST%' then 'US_MAJOR'
            when upper(airline) like '%LUFTHANSA%' or upper(airline) like '%AIR FRANCE%' 
                 or upper(airline) like '%BRITISH%' or upper(airline) like '%KLM%' then 'EUROPEAN'
            when upper(airline) like '%EMIRATES%' or upper(airline) like '%QATAR%' 
                 or upper(airline) like '%ETIHAD%' then 'MIDDLE_EAST'
            when upper(airline) like '%SINGAPORE%' or upper(airline) like '%CATHAY%' 
                 or upper(airline) like '%ANA%' or upper(airline) like '%JAL%' then 'ASIAN'
            else 'OTHER'
        end as airline_region,
        
        -- Data quality flags
        case 
            when departure_time is not null and departure_time != '' then true
            else false
        end as has_departure_time,
        
        case 
            when arrival_time is not null and arrival_time != '' then true
            else false
        end as has_arrival_time,
        
        case 
            when duration is not null and duration > 0 then true
            else false
        end as has_valid_duration,
        
        case 
            when price is not null and price > 0 then true
            else false
        end as has_pricing_data,
        
        case 
            when source_city is not null and source_city != '' 
                 and destination_city is not null and destination_city != '' then true
            else false
        end as has_complete_route
        
    from flights_with_stops_parsed
)

select * from flights_cleaned
