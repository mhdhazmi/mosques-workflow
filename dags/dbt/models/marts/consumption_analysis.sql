{{ config(
    materialized='incremental',
    unique_key=['meter_id', 'quarter'],
    on_schema_change='append_new_columns'
) }}

with source_data as (
    select * from {{ ref('int_meter_readings_with_periods') }}
),

-- Identify which meter/quarter combinations need processing
{% if is_incremental() %}
source_quarters as (
    -- Get max reading date per meter/quarter from source
    select
        meter_id,
        quarter,
        MAX(reading_date) as source_max_date
    from source_data
    group by meter_id, quarter
),

target_quarters as (
    -- Get max reading date per meter/quarter from existing target
    select
        meter_id,
        quarter,
        max_reading_date as target_max_date
    from {{ this }}
),

quarters_to_process as (
    -- Process if: new quarter OR source has newer data than target
    select s.meter_id, s.quarter
    from source_quarters s
    left join target_quarters t
        on s.meter_id = t.meter_id and s.quarter = t.quarter
    where t.meter_id IS NULL  -- New meter/quarter combination
       OR s.source_max_date > t.target_max_date  -- Source has newer data
),
{% endif %}

period_data as (
    select s.*
    from source_data s
    {% if is_incremental() %}
    where EXISTS (
        select 1 from quarters_to_process q
        where s.meter_id = q.meter_id and s.quarter = q.quarter
    )
    {% endif %}
),

meter_info as (
    select * from {{ ref('int_meter_locations') }}
),

aggregated as (
    select
        p.meter_id,

        -- Total Consumption (All readings, regardless of period)
        -- This ensures we capture all meter activity even if not in morning/evening periods
        ROUND(AVG(p.active_power_watts), 3) as total_avg_consumption,
        ROUND(SUM(p.active_power_watts), 3) as total_sum_consumption,
        COUNT(p.active_power_watts) as total_reading_count,

        -- Morning Stats (Exclude Fridays - Jumah prayer day)
        ROUND(AVG(CASE WHEN
                (p.reading_time BETWEEN p.morning_start_time AND p.morning_end_time)
                AND EXTRACT(DAYOFWEEK FROM p.reading_date) != 6
            THEN p.active_power_watts END), 3) as morning_avg_consumption,

        ROUND(SUM(CASE WHEN
                (p.reading_time BETWEEN p.morning_start_time AND p.morning_end_time)
                AND EXTRACT(DAYOFWEEK FROM p.reading_date) != 6
            THEN p.active_power_watts END), 3) as morning_sum_consumption,

        COUNT(CASE WHEN
                (p.reading_time BETWEEN p.morning_start_time AND p.morning_end_time)
                AND EXTRACT(DAYOFWEEK FROM p.reading_date) != 6
                AND p.active_power_watts IS NOT NULL
            THEN 1 END) as morning_reading_count,

        -- Evening Stats (wraps midnight: after Isha to before Fajr)
        ROUND(AVG(CASE WHEN
                (p.reading_time >= p.evening_start_time OR p.reading_time <= p.evening_end_time)
            THEN p.active_power_watts END), 3) as evening_avg_consumption,

        ROUND(SUM(CASE WHEN
                (p.reading_time >= p.evening_start_time OR p.reading_time <= p.evening_end_time)
            THEN p.active_power_watts END), 3) as evening_sum_consumption,

        COUNT(CASE WHEN
                (p.reading_time >= p.evening_start_time OR p.reading_time <= p.evening_end_time)
                AND p.active_power_watts IS NOT NULL
            THEN 1 END) as evening_reading_count,

        -- Metadata Columns
        MIN(p.reading_date) as min_reading_date,
        MAX(p.reading_date) as max_reading_date,
        COUNT(p.morning_start_time) as readings_with_prayer_times,
        p.quarter

    from period_data p
    group by p.meter_id, p.quarter
)

select
    a.meter_id,

    -- Total Consumption Metrics
    a.total_avg_consumption,
    a.total_sum_consumption,
    a.total_reading_count,

    -- Period-Specific Consumption
    a.morning_avg_consumption,
    a.morning_sum_consumption,
    a.morning_reading_count,
    a.evening_avg_consumption,
    a.evening_sum_consumption,
    a.evening_reading_count,

    -- Metadata
    a.min_reading_date,
    a.max_reading_date,
    a.readings_with_prayer_times,
    a.quarter,

    -- Meter Info
    m.multiplication_factor,
    m.region,
    m.province,

    -- Energy Calculations with Multiplication Factor
    -- Note: Readings are 30-min intervals, so divide by 2 to get hourly rate
    -- Formula: (Sum_Watts * MF / 2) / 1000 = kWh

    -- Total Energy
    ROUND((a.total_sum_consumption * m.multiplication_factor / 2) / 1000, 3) as total_energy_kwh,

    -- Period Energy
    ROUND((a.morning_sum_consumption * m.multiplication_factor / 2) / 1000, 3) as morning_energy_kwh,
    ROUND((a.evening_sum_consumption * m.multiplication_factor / 2) / 1000, 3) as evening_energy_kwh,

    -- Cost Calculations (SAR per kWh from config)
    ROUND(((a.total_sum_consumption * m.multiplication_factor / 2) / 1000) * {{ var('electricity_rate_sar') }}, 3) as total_cost_sar,
    ROUND(((a.morning_sum_consumption * m.multiplication_factor / 2) / 1000) * {{ var('electricity_rate_sar') }}, 3) as morning_cost_sar,
    ROUND(((a.evening_sum_consumption * m.multiplication_factor / 2) / 1000) * {{ var('electricity_rate_sar') }}, 3) as evening_cost_sar,

    -- Data Quality Flag
    CASE
        WHEN a.morning_avg_consumption IS NULL AND a.evening_avg_consumption IS NULL THEN 'NO_PERIOD_DATA'
        WHEN a.morning_avg_consumption IS NULL THEN 'NO_MORNING_DATA'
        WHEN a.evening_avg_consumption IS NULL THEN 'NO_EVENING_DATA'
        ELSE 'COMPLETE'
    END as data_quality_flag

from aggregated a
left join meter_info m on CAST(a.meter_id as STRING) = CAST(m.meter_id as STRING)

-- Filter out meters with no period-specific consumption data
-- Keep meters with at least ONE period (morning OR evening) with data
-- This removes 1.3% of meters (369) that have no prayer time matches
where a.morning_avg_consumption IS NOT NULL
   OR a.evening_avg_consumption IS NOT NULL
