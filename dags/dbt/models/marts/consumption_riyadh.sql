{{ config(
    materialized='table'
) }}

-- Riyadh Consumption Analysis
-- All Riyadh meters with quality filter and over-consumer flags
-- This includes BOTH violators AND compliant meters for full reporting

with consumption as (
    select * from {{ ref('consumption_analysis') }}
),

quality as (
    select * from {{ ref('int_meter_quality') }}
),

riyadh_meters as (
    select
        c.meter_id,
        c.quarter,

        -- Consumption metrics
        c.morning_avg_consumption,
        c.morning_sum_consumption,
        c.morning_reading_count,
        c.evening_avg_consumption,
        c.evening_sum_consumption,
        c.evening_reading_count,

        -- Energy and cost
        c.morning_energy_kwh,
        c.evening_energy_kwh,
        c.total_energy_kwh,
        c.morning_cost_sar,
        c.evening_cost_sar,
        c.total_cost_sar,

        -- Meter info
        c.multiplication_factor,
        c.region,
        c.province,
        c.min_reading_date,
        c.max_reading_date,

        -- Data quality metrics
        q.quality_percentage,
        q.actual_readings,
        q.zero_readings,
        q.missing_readings,
        q.is_good_quality,

        -- Calculate average consumption with multiplication factor
        c.morning_avg_consumption * c.multiplication_factor as morning_avg_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_avg_mf,

        -- Over-consumer flags (>3000W threshold)
        CASE
            WHEN c.morning_avg_consumption * c.multiplication_factor > 3000
            THEN TRUE ELSE FALSE
        END as over_in_morning,

        CASE
            WHEN c.evening_avg_consumption * c.multiplication_factor > 3000
            THEN TRUE ELSE FALSE
        END as over_in_evening,

        CASE
            WHEN c.morning_avg_consumption * c.multiplication_factor > 3000
                AND c.evening_avg_consumption * c.multiplication_factor > 3000
            THEN TRUE ELSE FALSE
        END as over_in_both,

        CASE
            WHEN c.morning_avg_consumption * c.multiplication_factor > 3000
                OR c.evening_avg_consumption * c.multiplication_factor > 3000
            THEN TRUE ELSE FALSE
        END as over_in_either

    from consumption c
    inner join quality q on c.meter_id = q.meter_id
    -- Riyadh filter
    where c.region = 'Central' 
      and c.province like '%RIYADH%'
      -- Quality filter: only include meters with good data (>50% quality)
      and q.is_good_quality = TRUE
)

select * from riyadh_meters

