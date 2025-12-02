with source as (
    select * from {{ ref('prayer_times') }}
),

renamed as (
    select
        -- Parse Coordinate string "(lat, lon)" to Geography Point
        -- Assumes format is like "(24.7136, 46.6753)"
        ST_GEOGPOINT(
            CAST(SPLIT(REPLACE(REPLACE(Coordinate, '(', ''), ')', ''), ',')[OFFSET(1)] AS FLOAT64), -- Longitude (x)
            CAST(SPLIT(REPLACE(REPLACE(Coordinate, '(', ''), ')', ''), ',')[OFFSET(0)] AS FLOAT64)  -- Latitude (y)
        ) as location,
        Coordinate as original_coordinate,
        
        -- Cast Date
        PARSE_DATE('%d-%m-%Y', Date) as date,
        
        -- Prayer Times
        PARSE_TIME('%H:%M', Fajr) as fajr_time,
        PARSE_TIME('%H:%M', Dhuhr) as dhuhr_time,
        PARSE_TIME('%H:%M', Asr) as asr_time,
        PARSE_TIME('%H:%M', Maghrib) as maghrib_time,
        PARSE_TIME('%H:%M', Isha) as isha_time

    from source
)

select * from renamed
