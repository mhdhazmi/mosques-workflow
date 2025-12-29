{{ config(
    materialized='table',
    description='Percentile ranking for each meter showing position relative to all meters'
) }}

-- meter_percentile_rank: Shows where each meter stands compared to others
-- "Your mosque consumes more than X% of mosques"

with consumption as (
    select * from {{ ref('consumption_analysis') }}
    where data_quality_flag = 'COMPLETE'
),

quality as (
    select * from {{ ref('int_meter_quality') }}
    where is_good_quality = TRUE
),

-- Get overall benchmarks for reference
benchmarks as (
    select *
    from {{ ref('consumption_benchmarks') }}
    where benchmark_level = 'overall'
    limit 1
),

-- Calculate percentile ranks
ranked as (
    select
        c.meter_id,
        c.quarter,
        c.region,
        c.province,
        c.multiplication_factor,

        -- Consumption with MF
        c.morning_avg_consumption * c.multiplication_factor as morning_avg_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_avg_mf,
        c.total_avg_consumption * c.multiplication_factor as total_avg_mf,

        -- Percentile ranks (0-1 scale)
        ROUND(PERCENT_RANK() OVER (ORDER BY c.morning_avg_consumption * c.multiplication_factor), 4) as morning_percentile_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY c.evening_avg_consumption * c.multiplication_factor), 4) as evening_percentile_rank,
        ROUND(PERCENT_RANK() OVER (ORDER BY c.total_avg_consumption * c.multiplication_factor), 4) as total_percentile_rank,

        -- Percentile as integer (0-100)
        CAST(ROUND(PERCENT_RANK() OVER (ORDER BY c.morning_avg_consumption * c.multiplication_factor) * 100, 0) AS INT64) as morning_percentile,
        CAST(ROUND(PERCENT_RANK() OVER (ORDER BY c.evening_avg_consumption * c.multiplication_factor) * 100, 0) AS INT64) as evening_percentile,
        CAST(ROUND(PERCENT_RANK() OVER (ORDER BY c.total_avg_consumption * c.multiplication_factor) * 100, 0) AS INT64) as total_percentile,

        -- Regional percentile ranks
        ROUND(PERCENT_RANK() OVER (PARTITION BY c.region ORDER BY c.morning_avg_consumption * c.multiplication_factor), 4) as regional_morning_percentile_rank,
        ROUND(PERCENT_RANK() OVER (PARTITION BY c.region ORDER BY c.evening_avg_consumption * c.multiplication_factor), 4) as regional_evening_percentile_rank,

        -- Rank within region
        CAST(ROUND(PERCENT_RANK() OVER (PARTITION BY c.region ORDER BY c.total_avg_consumption * c.multiplication_factor) * 100, 0) AS INT64) as regional_percentile,

        -- Quality
        q.quality_percentage

    from consumption c
    inner join quality q
        on c.meter_id = q.meter_id and c.quarter = q.quarter
)

select
    r.*,

    -- Benchmark comparison (overall)
    b.morning_p50 as benchmark_morning_median,
    b.evening_p50 as benchmark_evening_median,

    -- Difference from median
    ROUND(r.morning_avg_mf - b.morning_p50, 3) as morning_diff_from_median,
    ROUND(r.evening_avg_mf - b.evening_p50, 3) as evening_diff_from_median,

    -- Percentage above/below median
    ROUND((r.morning_avg_mf - b.morning_p50) / b.morning_p50 * 100, 2) as morning_pct_from_median,
    ROUND((r.evening_avg_mf - b.evening_p50) / b.evening_p50 * 100, 2) as evening_pct_from_median,

    -- Position description
    CASE
        WHEN r.total_percentile <= 10 THEN 'Top 10% Most Efficient'
        WHEN r.total_percentile <= 25 THEN 'Very Efficient (Top 25%)'
        WHEN r.total_percentile <= 50 THEN 'Below Median (Better than average)'
        WHEN r.total_percentile <= 75 THEN 'Above Median'
        WHEN r.total_percentile <= 90 THEN 'High Consumer (Top 25%)'
        ELSE 'Very High Consumer (Top 10%)'
    END as consumption_position

from ranked r
cross join benchmarks b
