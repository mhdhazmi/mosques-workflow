{{ config(
    materialized='table',
    description='Quarter-over-quarter consumption trend analysis for each meter'
) }}

-- meter_consumption_trend: Tracks consumption changes over time
-- Detects increasing/decreasing trends and sudden spikes

with consumption as (
    select * from {{ ref('consumption_analysis') }}
    where data_quality_flag = 'COMPLETE'
),

quality as (
    select * from {{ ref('int_meter_quality') }}
    where is_good_quality = TRUE
),

-- Add previous quarter consumption using LAG
with_previous as (
    select
        c.meter_id,
        c.quarter,
        c.region,
        c.province,
        c.multiplication_factor,

        -- Current consumption
        c.morning_avg_consumption * c.multiplication_factor as morning_avg_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_avg_mf,
        c.total_avg_consumption * c.multiplication_factor as total_avg_mf,

        -- Previous quarter consumption (LAG)
        LAG(c.morning_avg_consumption * c.multiplication_factor)
            OVER (PARTITION BY c.meter_id ORDER BY c.quarter) as prev_morning_avg_mf,
        LAG(c.evening_avg_consumption * c.multiplication_factor)
            OVER (PARTITION BY c.meter_id ORDER BY c.quarter) as prev_evening_avg_mf,
        LAG(c.total_avg_consumption * c.multiplication_factor)
            OVER (PARTITION BY c.meter_id ORDER BY c.quarter) as prev_total_avg_mf,

        LAG(c.quarter) OVER (PARTITION BY c.meter_id ORDER BY c.quarter) as prev_quarter,

        -- Quality
        q.quality_percentage,

        -- Count of quarters for this meter
        COUNT(*) OVER (PARTITION BY c.meter_id) as quarters_count

    from consumption c
    inner join quality q
        on c.meter_id = q.meter_id and c.quarter = q.quarter
),

-- Calculate changes and trends
trends as (
    select
        *,

        -- Absolute change from previous quarter
        ROUND(morning_avg_mf - COALESCE(prev_morning_avg_mf, morning_avg_mf), 3) as morning_change,
        ROUND(evening_avg_mf - COALESCE(prev_evening_avg_mf, evening_avg_mf), 3) as evening_change,
        ROUND(total_avg_mf - COALESCE(prev_total_avg_mf, total_avg_mf), 3) as total_change,

        -- Percentage change from previous quarter
        CASE
            WHEN prev_morning_avg_mf IS NULL OR prev_morning_avg_mf = 0 THEN NULL
            ELSE ROUND((morning_avg_mf - prev_morning_avg_mf) / prev_morning_avg_mf * 100, 2)
        END as morning_pct_change,

        CASE
            WHEN prev_evening_avg_mf IS NULL OR prev_evening_avg_mf = 0 THEN NULL
            ELSE ROUND((evening_avg_mf - prev_evening_avg_mf) / prev_evening_avg_mf * 100, 2)
        END as evening_pct_change,

        CASE
            WHEN prev_total_avg_mf IS NULL OR prev_total_avg_mf = 0 THEN NULL
            ELSE ROUND((total_avg_mf - prev_total_avg_mf) / prev_total_avg_mf * 100, 2)
        END as total_pct_change

    from with_previous
)

select
    t.*,

    -- Trend direction (morning)
    CASE
        WHEN prev_morning_avg_mf IS NULL THEN 'FIRST_QUARTER'
        WHEN morning_pct_change > 50 THEN 'SPIKE'
        WHEN morning_pct_change > 10 THEN 'INCREASING'
        WHEN morning_pct_change < -50 THEN 'DROP'
        WHEN morning_pct_change < -10 THEN 'DECREASING'
        ELSE 'STABLE'
    END as morning_trend,

    -- Trend direction (evening)
    CASE
        WHEN prev_evening_avg_mf IS NULL THEN 'FIRST_QUARTER'
        WHEN evening_pct_change > 50 THEN 'SPIKE'
        WHEN evening_pct_change > 10 THEN 'INCREASING'
        WHEN evening_pct_change < -50 THEN 'DROP'
        WHEN evening_pct_change < -10 THEN 'DECREASING'
        ELSE 'STABLE'
    END as evening_trend,

    -- Overall trend
    CASE
        WHEN prev_total_avg_mf IS NULL THEN 'FIRST_QUARTER'
        WHEN total_pct_change > 50 THEN 'SPIKE'
        WHEN total_pct_change > 10 THEN 'INCREASING'
        WHEN total_pct_change < -50 THEN 'DROP'
        WHEN total_pct_change < -10 THEN 'DECREASING'
        ELSE 'STABLE'
    END as overall_trend,

    -- Alert flags
    CASE
        WHEN total_pct_change > 50 THEN TRUE
        ELSE FALSE
    END as has_spike,

    CASE
        WHEN total_pct_change < -50 THEN TRUE
        ELSE FALSE
    END as has_drop,

    -- Possible cause hints for spikes
    CASE
        WHEN total_pct_change > 50 THEN
            CASE
                WHEN quarter LIKE '%-Q2' OR quarter LIKE '%-Q3' THEN 'SEASONAL_COOLING_LIKELY'
                ELSE 'NEW_EQUIPMENT_OR_ISSUE'
            END
        WHEN total_pct_change < -50 THEN 'EQUIPMENT_OFF_OR_FIXED'
        ELSE NULL
    END as change_hint

from trends
