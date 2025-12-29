{{ config(
    materialized='table',
    description='6-tier consumption classification for each meter based on benchmark percentiles'
) }}

-- meter_classification: Assigns consumption tier to each meter based on overall benchmarks
-- Tiers: EFFICIENT, NORMAL_LOW, NORMAL_HIGH, ELEVATED, HIGH, VIOLATOR

with consumption as (
    select * from {{ ref('consumption_analysis') }}
    where data_quality_flag = 'COMPLETE'
),

quality as (
    select * from {{ ref('int_meter_quality') }}
    where is_good_quality = TRUE
),

-- Get overall benchmarks
benchmarks as (
    select *
    from {{ ref('consumption_benchmarks') }}
    where benchmark_level = 'overall'
    limit 1
),

-- Join consumption with benchmarks for classification
classified as (
    select
        c.meter_id,
        c.quarter,
        c.region,
        c.province,
        c.multiplication_factor,

        -- Consumption metrics
        c.morning_avg_consumption,
        c.evening_avg_consumption,
        c.total_avg_consumption,
        c.morning_avg_consumption * c.multiplication_factor as morning_avg_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_avg_mf,
        c.total_avg_consumption * c.multiplication_factor as total_avg_mf,

        -- Reading counts
        c.morning_reading_count,
        c.evening_reading_count,
        c.total_reading_count,

        -- Energy and cost
        c.morning_energy_kwh,
        c.evening_energy_kwh,
        c.total_energy_kwh,
        c.total_cost_sar,

        -- Quality
        q.quality_percentage,

        -- Benchmark values for reference
        b.morning_p25,
        b.morning_p50,
        b.morning_p75,
        b.morning_p90,
        b.evening_p25,
        b.evening_p50,
        b.evening_p75,
        b.evening_p90,

        -- Morning tier classification
        CASE
            WHEN c.morning_avg_consumption * c.multiplication_factor <= b.morning_p25
                THEN 'EFFICIENT'
            WHEN c.morning_avg_consumption * c.multiplication_factor <= b.morning_p50
                THEN 'NORMAL_LOW'
            WHEN c.morning_avg_consumption * c.multiplication_factor <= b.morning_p75
                THEN 'NORMAL_HIGH'
            WHEN c.morning_avg_consumption * c.multiplication_factor <= b.morning_p90
                THEN 'ELEVATED'
            WHEN c.morning_avg_consumption * c.multiplication_factor <= {{ var('violation_threshold_watts') }}
                THEN 'HIGH'
            ELSE 'VIOLATOR'
        END as morning_tier,

        -- Evening tier classification
        CASE
            WHEN c.evening_avg_consumption * c.multiplication_factor <= b.evening_p25
                THEN 'EFFICIENT'
            WHEN c.evening_avg_consumption * c.multiplication_factor <= b.evening_p50
                THEN 'NORMAL_LOW'
            WHEN c.evening_avg_consumption * c.multiplication_factor <= b.evening_p75
                THEN 'NORMAL_HIGH'
            WHEN c.evening_avg_consumption * c.multiplication_factor <= b.evening_p90
                THEN 'ELEVATED'
            WHEN c.evening_avg_consumption * c.multiplication_factor <= {{ var('violation_threshold_watts') }}
                THEN 'HIGH'
            ELSE 'VIOLATOR'
        END as evening_tier,

        -- Overall tier (based on highest consumption period)
        CASE
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > {{ var('violation_threshold_watts') }}
                THEN 'VIOLATOR'
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > b.evening_p90
                THEN 'HIGH'
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > b.evening_p75
                THEN 'ELEVATED'
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > b.evening_p50
                THEN 'NORMAL_HIGH'
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > b.evening_p25
                THEN 'NORMAL_LOW'
            ELSE 'EFFICIENT'
        END as overall_tier,

        -- Numeric tier for sorting (1=best, 6=worst)
        CASE
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > {{ var('violation_threshold_watts') }}
                THEN 6
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > b.evening_p90
                THEN 5
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > b.evening_p75
                THEN 4
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > b.evening_p50
                THEN 3
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > b.evening_p25
                THEN 2
            ELSE 1
        END as tier_rank,

        -- Is violator flag
        CASE
            WHEN c.morning_avg_consumption * c.multiplication_factor > {{ var('violation_threshold_watts') }}
                OR c.evening_avg_consumption * c.multiplication_factor > {{ var('violation_threshold_watts') }}
            THEN TRUE
            ELSE FALSE
        END as is_violator,

        -- Needs attention flag (ELEVATED or worse)
        CASE
            WHEN GREATEST(
                c.morning_avg_consumption * c.multiplication_factor,
                c.evening_avg_consumption * c.multiplication_factor
            ) > b.evening_p75
            THEN TRUE
            ELSE FALSE
        END as needs_attention

    from consumption c
    inner join quality q
        on c.meter_id = q.meter_id and c.quarter = q.quarter
    cross join benchmarks b
)

select * from classified
