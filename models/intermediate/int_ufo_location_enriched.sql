{{
  config(
    materialized='table'
  )
}}

with cleaned_sightings as (
    select * from {{ ref('int_ufo_sightings_cleaned') }}
),

location_stats as (
    select
        country_standardized,
        state_clean,
        city_clean,
        count(*) as city_sighting_count,
        min(sighting_date) as first_sighting_date,
        max(sighting_date) as last_sighting_date,
        avg(latitude_clean) as avg_latitude,
        avg(longitude_clean) as avg_longitude,
        count(case when has_valid_coordinates then 1 end) as sightings_with_coordinates
    from cleaned_sightings
    where has_valid_datetime = true
    group by country_standardized, state_clean, city_clean
),

enriched_sightings as (
    select 
        cs.*,
        co.lookup_country as coord_lookup_country,
        co.lookup_state as coord_lookup_state,
        ls.city_sighting_count,
        ls.first_sighting_date as city_first_sighting,
        ls.last_sighting_date as city_last_sighting,
        ls.avg_latitude as city_avg_latitude,
        ls.avg_longitude as city_avg_longitude,
        
        -- Add location activity categories
        case 
            when ls.city_sighting_count >= 100 then 'VERY HIGH ACTIVITY'
            when ls.city_sighting_count >= 50 then 'HIGH ACTIVITY'
            when ls.city_sighting_count >= 20 then 'MEDIUM ACTIVITY'
            when ls.city_sighting_count >= 5 then 'LOW ACTIVITY'
            else 'MINIMAL ACTIVITY'
        end as city_activity_level,
        
        -- Add regional groupings for better dashboard filtering
        case 
            when cs.country_standardized = 'UNITED STATES' then
                case 
                    when cs.state_clean in ('CA', 'OR', 'WA', 'NV', 'AK', 'HI') then 'US West'
                    when cs.state_clean in ('TX', 'NM', 'AZ', 'OK', 'AR', 'LA') then 'US Southwest'
                    when cs.state_clean in ('FL', 'GA', 'SC', 'NC', 'VA', 'WV', 'KY', 'TN', 'AL', 'MS') then 'US Southeast'
                    when cs.state_clean in ('NY', 'NJ', 'PA', 'CT', 'RI', 'MA', 'VT', 'NH', 'ME', 'MD', 'DE', 'DC') then 'US Northeast'
                    when cs.state_clean in ('IL', 'IN', 'OH', 'MI', 'WI', 'MN', 'IA', 'MO', 'ND', 'SD', 'NE', 'KS') then 'US Midwest'
                    when cs.state_clean in ('MT', 'WY', 'CO', 'UT', 'ID') then 'US Mountain'
                    else 'US Other'
                end
            when cs.country_standardized = 'CANADA' then 'Canada'
            when cs.country_standardized = 'UNITED KINGDOM' then 'United Kingdom'
            when cs.country_standardized in ('GERMANY', 'FRANCE', 'ITALY', 'SPAIN', 'NETHERLANDS', 'BELGIUM', 'SWITZERLAND', 'AUSTRIA') then 'Western Europe'
            when cs.country_standardized = 'AUSTRALIA' then 'Australia'
            else 'Other International'
        end as region
        
    from cleaned_sightings cs
    left join location_stats ls 
        on cs.country_standardized = ls.country_standardized
        and cs.state_clean = ls.state_clean
        and cs.city_clean = ls.city_clean
        left join (
                select lat_bucket, lon_bucket, coord_country as lookup_country, coord_usa_state as lookup_state
                from {{ ref('int_coordinates_lookup') }}
        ) co
            on round(cs.latitude_clean,1) = co.lat_bucket and round(cs.longitude_clean,1) = co.lon_bucket
)

select * from enriched_sightings
