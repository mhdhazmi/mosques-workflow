{{ config(
    materialized='view'
) }}

-- Calculate data quality score for each meter
-- Quality = (actual_readings - zero_readings) / expected_readings * 100
-- Expected readings = 48 per day (30-minute intervals)

with meter_stats as (
    select
        meter_id,
        MIN(reading_date) as min_date,
        MAX(reading_date) as max_date,
        DATE_DIFF(MAX(reading_date), MIN(reading_date), DAY) as date_range_days,

        -- Expected readings: 48 per day (30-min intervals)
        DATE_DIFF(MAX(reading_date), MIN(reading_date), DAY) * 48 as expected_readings,

        -- Actual readings count
        COUNT(*) as actual_readings,

        -- Zero readings count
        SUM(CASE WHEN active_power_watts = 0 THEN 1 ELSE 0 END) as zero_readings,

        -- NULL readings (already filtered in staging but check anyway)
        SUM(CASE WHEN active_power_watts IS NULL THEN 1 ELSE 0 END) as null_readings

    -- Use int_exclude_ramadan to filter out Ramadan dates when configured
    from {{ ref('int_exclude_ramadan') }}
    group by meter_id
)

select
    meter_id,
    min_date,
    max_date,
    date_range_days,
    expected_readings,
    actual_readings,
    zero_readings,
    null_readings,

    -- Missing readings (expected - actual)
    expected_readings - actual_readings as missing_readings,

    -- Quality percentage (good readings / expected)
    CASE
        WHEN expected_readings > 0 THEN
            ROUND((actual_readings - zero_readings) / CAST(expected_readings AS FLOAT64) * 100, 2)
        ELSE 0
    END as quality_percentage,

    -- Quality flag (good if >= 50%)
    CASE
        WHEN expected_readings > 0
            AND (actual_readings - zero_readings) / CAST(expected_readings AS FLOAT64) * 100 >= 50
        THEN TRUE
        ELSE FALSE
    END as is_good_quality

from meter_stats
where expected_readings > 0  -- Avoid division by zero
