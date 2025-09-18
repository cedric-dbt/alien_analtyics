with ufo_sightings as (
    select
        datetime,
        city,
        state,
        country,
        shape,
        duration_seconds as duration,
        floor(duration_seconds / 3600) as duration_hours,
        floor((duration_seconds % 3600) / 60) as duration_minutes,
        comments,
        latitude,
        longitude
    from {{ source('ufo_raw', 'ufo_sightings_raw') }}
)

select * from ufo_sightings