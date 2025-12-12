with readings as (
    -- Use int_exclude_ramadan to filter out Ramadan dates when configured
    select * from {{ ref('int_exclude_ramadan') }}
),

prayer_times as (
    select * from {{ ref('stg_prayer_times') }}
),

-- Get the nearest prayer location for each meter
meter_locations as (
    select
        CAST(meter_id as STRING) as meter_id,
        nearest_prayer_coordinate
    from {{ ref('int_meter_locations') }}
),

joined as (
    select
        r.meter_id,
        r.reading_at,
        r.active_power_watts,
        r.reading_date,
        r.reading_time,
        r.quarter,

        p.fajr_time,
        p.dhuhr_time,
        p.isha_time,

        -- Calculate Period Margins (in minutes)
        -- Morning Start: Fajr + 100 mins (20+20+30+20+10)
        -- Morning End: Dhuhr - 80 mins (60+20)
        -- Evening Start: Isha + 90 mins (20+20+30+20)
        -- Evening End: Fajr - 80 mins (60+20)

        TIME_ADD(p.fajr_time, INTERVAL 100 MINUTE) as morning_start_time,
        TIME_SUB(p.dhuhr_time, INTERVAL 80 MINUTE) as morning_end_time,
        TIME_ADD(p.isha_time, INTERVAL 90 MINUTE) as evening_start_time,
        TIME_SUB(p.fajr_time, INTERVAL 80 MINUTE) as evening_end_time

    from readings r
    -- First join to get the nearest prayer coordinate for this meter
    left join meter_locations m on CAST(r.meter_id as STRING) = m.meter_id
    -- Then join prayer times on month/day (prayer times repeat yearly) AND the nearest coordinate
    left join prayer_times p
        on EXTRACT(MONTH FROM r.reading_date) = EXTRACT(MONTH FROM p.date)
        and EXTRACT(DAY FROM r.reading_date) = EXTRACT(DAY FROM p.date)
        and m.nearest_prayer_coordinate = p.original_coordinate
)

select * from joined
