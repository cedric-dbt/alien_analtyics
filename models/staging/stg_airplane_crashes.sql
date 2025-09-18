{{
  config(
    materialized='view'
  )
}}

with crashes_raw as (
    select * from {{ source('ufo_raw', 'airplane_crashes_since_1908') }}
),

crashes_cleaned as (
    select
        -- Time parsing and standardization
        case 
            when date is not null and trim(date) != '' and trim(date) != 'c: '
            then try_to_date(date)
            else null 
        end as crash_date,
        
        year(case 
            when date is not null and trim(date) != '' and trim(date) != 'c: '
            then try_to_date(date)
            else null 
        end) as crash_year,
        month(case 
            when date is not null and trim(date) != '' and trim(date) != 'c: '
            then try_to_date(date)
            else null 
        end) as crash_month,
        day(case 
            when date is not null and trim(date) != '' and trim(date) != 'c: '
            then try_to_date(date)
            else null 
        end) as crash_day,
        dayofweek(case 
            when date is not null and trim(date) != '' and trim(date) != 'c: '
            then try_to_date(date)
            else null 
        end) as crash_day_of_week,
        
        -- Time parsing (if available)
        case 
            when time is not null and time != '' and time != 'c: ' 
            then trim(time)
            else null 
        end as crash_time_raw,
        
        -- Location cleaning
        trim(coalesce(location, 'UNKNOWN')) as crash_location,
        
        -- Extract country from location (usually at the end)
        case 
            when location like '%, %' then 
                trim(split_part(location, ',', -1))  -- Last part after comma
            else 'UNKNOWN'
        end as crash_country,
        
        -- Aircraft and operator info
        trim(upper(coalesce(operator, 'UNKNOWN'))) as airline_operator,
        trim(coalesce(flight_number, '')) as flight_number_clean,
        trim(coalesce(route, '')) as flight_route,
        trim(upper(coalesce(type, 'UNKNOWN'))) as aircraft_type,
        trim(upper(coalesce(registration, ''))) as aircraft_registration,
        trim(coalesce(cn_ln, '')) as construction_number,
        
        -- Casualty data with validation
        case 
            when aboard >= 0 then aboard
            else null 
        end as people_aboard,
        
        case 
            when fatalities >= 0 then fatalities
            else null 
        end as total_fatalities,
        
        case 
            when ground >= 0 then ground
            else null 
        end as ground_fatalities,
        
        -- Calculate survival metrics
        case 
            when aboard > 0 and fatalities >= 0 
            then aboard - fatalities
            else null 
        end as survivors,
        
        case 
            when aboard > 0 and fatalities >= 0 
            then round((fatalities * 100.0 / aboard), 2)
            else null 
        end as fatality_rate_percent,
        
        -- Summary text cleaning
        case 
            when summary is not null and summary != '' 
            then trim(summary)
            else null 
        end as crash_summary,
        
        -- Aircraft type categorization
        case 
            when upper(type) like '%BOEING%' or upper(type) like '%B-%' then 'BOEING'
            when upper(type) like '%AIRBUS%' or upper(type) like '%A-%' then 'AIRBUS'
            when upper(type) like '%DOUGLAS%' or upper(type) like '%DC-%' or upper(type) like '%MD-%' then 'DOUGLAS'
            when upper(type) like '%CESSNA%' then 'CESSNA'
            when upper(type) like '%PIPER%' then 'PIPER'
            when upper(type) like '%LOCKHEED%' then 'LOCKHEED'
            when upper(type) like '%MILITARY%' or upper(type) like '%FIGHTER%' then 'MILITARY'
            else 'OTHER'
        end as aircraft_manufacturer,
        
        -- Operator type categorization
        case 
            when upper(operator) like '%MILITARY%' or upper(operator) like '%AIR FORCE%' 
                 or upper(operator) like '%NAVY%' or upper(operator) like '%ARMY%' then 'MILITARY'
            when upper(operator) like '%PRIVATE%' or upper(operator) like '%PERSONAL%' then 'PRIVATE'
            when upper(operator) like '%CARGO%' or upper(operator) like '%FREIGHT%' then 'CARGO'
            when upper(operator) like '%CHARTER%' then 'CHARTER'
            when operator is not null and operator != 'UNKNOWN' then 'COMMERCIAL'
            else 'UNKNOWN'
        end as operator_type,
        
        -- Severity categorization
        case 
            when fatalities = 0 then 'NO_FATALITIES'
            when fatalities between 1 and 5 then 'LOW_CASUALTIES'
            when fatalities between 6 and 50 then 'MODERATE_CASUALTIES'
            when fatalities between 51 and 200 then 'HIGH_CASUALTIES'
            when fatalities > 200 then 'MASS_CASUALTY'
            else 'UNKNOWN'
        end as casualty_severity,
        
        -- Era categorization for historical analysis
        case 
            when year(case 
                when date is not null and trim(date) != '' and trim(date) != 'c: '
                then try_to_date(date)
                else null 
            end) < 1950 then 'EARLY_AVIATION'
            when year(case 
                when date is not null and trim(date) != '' and trim(date) != 'c: '
                then try_to_date(date)
                else null 
            end) < 1970 then 'JET_AGE_EARLY'
            when year(case 
                when date is not null and trim(date) != '' and trim(date) != 'c: '
                then try_to_date(date)
                else null 
            end) < 1990 then 'JET_AGE_MATURE'
            when year(case 
                when date is not null and trim(date) != '' and trim(date) != 'c: '
                then try_to_date(date)
                else null 
            end) < 2010 then 'MODERN_AVIATION'
            when year(case 
                when date is not null and trim(date) != '' and trim(date) != 'c: '
                then try_to_date(date)
                else null 
            end) >= 2010 then 'CONTEMPORARY'
            else 'UNKNOWN'
        end as aviation_era,
        
        -- Data quality flags
        case 
            when case 
                when date is not null and trim(date) != '' and trim(date) != 'c: '
                then try_to_date(date)
                else null 
            end is not null then true
            else false 
        end as has_valid_date,
        
        case 
            when aboard is not null and aboard > 0 then true
            else false 
        end as has_casualty_data,
        
        case 
            when location is not null and location != '' then true
            else false 
        end as has_location_data
        
    from crashes_raw
    where case 
        when date is not null and trim(date) != '' and trim(date) != 'c: '
        then try_to_date(date)
        else null 
    end is not null  -- Only include records with valid dates
)

select * from crashes_cleaned
