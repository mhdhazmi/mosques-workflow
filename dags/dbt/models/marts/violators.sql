{{ config(
    materialized='incremental',
    unique_key=['meter_id', 'quarter'],
    on_schema_change='append_new_columns'
) }}

-- Violators: Meters that consume >3000W during prayer periods
-- Only includes meters with good data quality (>50% non-missing/non-zero readings)

with consumption as (
    select * from {{ ref('consumption_analysis') }}
    {% if is_incremental() %}
        -- Only process meters/quarters that are new or have been updated
        WHERE (meter_id, quarter) NOT IN (
            SELECT meter_id, quarter FROM {{ this }}
        )
    {% endif %}
),

quality as (
    select * from {{ ref('int_meter_quality') }}
),

flagged as (
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

        -- Calculate average consumption with multiplication factor
        c.morning_avg_consumption * c.multiplication_factor as morning_avg_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_avg_mf,

        -- Over-consumer flags (>3000W threshold)
        CASE
            WHEN c.morning_avg_consumption * c.multiplication_factor > 3000
            THEN TRUE
            ELSE FALSE
        END as over_in_morning,

        CASE
            WHEN c.evening_avg_consumption * c.multiplication_factor > 3000
            THEN TRUE
            ELSE FALSE
        END as over_in_evening,

        -- Combined flags
        CASE
            WHEN c.morning_avg_consumption * c.multiplication_factor > 3000
                AND c.evening_avg_consumption * c.multiplication_factor > 3000
            THEN TRUE
            ELSE FALSE
        END as over_in_both,

        CASE
            WHEN c.morning_avg_consumption * c.multiplication_factor > 3000
                OR c.evening_avg_consumption * c.multiplication_factor > 3000
            THEN TRUE
            ELSE FALSE
        END as over_in_either,

        -- Violation category
        CASE
            WHEN c.morning_avg_consumption * c.multiplication_factor > 3000
                AND c.evening_avg_consumption * c.multiplication_factor > 3000
            THEN 'BOTH_PERIODS'
            WHEN c.morning_avg_consumption * c.multiplication_factor > 3000
            THEN 'MORNING_ONLY'
            WHEN c.evening_avg_consumption * c.multiplication_factor > 3000
            THEN 'EVENING_ONLY'
            ELSE 'COMPLIANT'
        END as violation_category

    from consumption c
    inner join quality q on c.meter_id = q.meter_id
    -- Quality filter: only include meters with good data (>50% quality)
    where q.is_good_quality = TRUE
)

select
    *,

    -- Calculate potential savings if over-consumers reduce to 500W (normal level)
    CASE
        WHEN over_in_morning = TRUE
        THEN ROUND(((morning_avg_mf - 500) * morning_reading_count / 2 / 1000) * 0.32, 3)
        ELSE 0
    END as potential_savings_morning_sar,

    CASE
        WHEN over_in_evening = TRUE
        THEN ROUND(((evening_avg_mf - 500) * evening_reading_count / 2 / 1000) * 0.32, 3)
        ELSE 0
    END as potential_savings_evening_sar,

    -- Total potential savings
    CASE
        WHEN over_in_morning = TRUE
        THEN ROUND(((morning_avg_mf - 500) * morning_reading_count / 2 / 1000) * 0.32, 3)
        ELSE 0
    END +
    CASE
        WHEN over_in_evening = TRUE
        THEN ROUND(((evening_avg_mf - 500) * evening_reading_count / 2 / 1000) * 0.32, 3)
        ELSE 0
    END as total_potential_savings_sar

from flagged

-- Only include violators (over-consumers in at least one period)
where over_in_either = TRUE
