{{ config(
    materialized='table',
    description='Efficiency score (0-100) for each meter, higher = more efficient'
) }}

-- meter_efficiency_score: Composite score based on consumption relative to benchmarks
-- Score 100 = most efficient (lowest consumption)
-- Score 0 = least efficient (highest consumption)

with consumption as (
    select * from {{ ref('consumption_analysis') }}
    where data_quality_flag = 'COMPLETE'
),

quality as (
    select * from {{ ref('int_meter_quality') }}
    where is_good_quality = TRUE
),

-- Get benchmarks for scoring
benchmarks as (
    select *
    from {{ ref('consumption_benchmarks') }}
    where benchmark_level = 'overall'
    limit 1
),

-- Calculate efficiency scores
scored as (
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

        -- Quality
        q.quality_percentage,

        -- Efficiency score: 100 * (1 - percentile_rank)
        -- Lower consumption = higher score
        ROUND(100 * (1 - PERCENT_RANK() OVER (ORDER BY c.morning_avg_consumption * c.multiplication_factor)), 1) as morning_efficiency_score,
        ROUND(100 * (1 - PERCENT_RANK() OVER (ORDER BY c.evening_avg_consumption * c.multiplication_factor)), 1) as evening_efficiency_score,
        ROUND(100 * (1 - PERCENT_RANK() OVER (ORDER BY c.total_avg_consumption * c.multiplication_factor)), 1) as total_efficiency_score,

        -- Regional efficiency score
        ROUND(100 * (1 - PERCENT_RANK() OVER (PARTITION BY c.region ORDER BY c.total_avg_consumption * c.multiplication_factor)), 1) as regional_efficiency_score,

        -- Benchmark values
        b.morning_p50 as benchmark_morning_median,
        b.evening_p50 as benchmark_evening_median,
        b.total_p50 as benchmark_total_median

    from consumption c
    inner join quality q
        on c.meter_id = q.meter_id and c.quarter = q.quarter
    cross join benchmarks b
)

select
    s.*,

    -- Overall efficiency score (average of morning and evening)
    ROUND((s.morning_efficiency_score + s.evening_efficiency_score) / 2, 1) as combined_efficiency_score,

    -- Efficiency grade
    CASE
        WHEN (s.morning_efficiency_score + s.evening_efficiency_score) / 2 >= 90 THEN 'A+'
        WHEN (s.morning_efficiency_score + s.evening_efficiency_score) / 2 >= 80 THEN 'A'
        WHEN (s.morning_efficiency_score + s.evening_efficiency_score) / 2 >= 70 THEN 'B'
        WHEN (s.morning_efficiency_score + s.evening_efficiency_score) / 2 >= 60 THEN 'C'
        WHEN (s.morning_efficiency_score + s.evening_efficiency_score) / 2 >= 50 THEN 'D'
        WHEN (s.morning_efficiency_score + s.evening_efficiency_score) / 2 >= 25 THEN 'E'
        ELSE 'F'
    END as efficiency_grade,

    -- Is efficient (top 25%)
    CASE
        WHEN (s.morning_efficiency_score + s.evening_efficiency_score) / 2 >= 75 THEN TRUE
        ELSE FALSE
    END as is_efficient,

    -- Is inefficient (bottom 25%)
    CASE
        WHEN (s.morning_efficiency_score + s.evening_efficiency_score) / 2 <= 25 THEN TRUE
        ELSE FALSE
    END as is_inefficient,

    -- Potential improvement (watts to reach median)
    GREATEST(0, ROUND(s.morning_avg_mf - s.benchmark_morning_median, 0)) as morning_excess_from_median,
    GREATEST(0, ROUND(s.evening_avg_mf - s.benchmark_evening_median, 0)) as evening_excess_from_median,

    -- Estimated savings if reduced to median (SAR)
    GREATEST(0, ROUND(
        ((s.morning_avg_mf - s.benchmark_morning_median) / 2 / 1000 +
         (s.evening_avg_mf - s.benchmark_evening_median) / 2 / 1000)
        * {{ var('electricity_rate_sar') }} * 30 * 48,  -- Approx monthly readings
        2
    )) as estimated_monthly_savings_if_at_median_sar

from scored s
