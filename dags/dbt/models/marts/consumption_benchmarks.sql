{{ config(
    materialized='table',
    description='Statistical benchmarks for normal mosque consumption patterns (excludes violators)'
) }}

-- consumption_benchmarks: Percentile statistics for compliant meters
-- Used as reference for classification, savings calculations, and anomaly detection

with consumption as (
    select * from {{ ref('consumption_analysis') }}
    where data_quality_flag = 'COMPLETE'
),

quality as (
    select * from {{ ref('int_meter_quality') }}
    where is_good_quality = TRUE
),

-- Compliant meters: good quality, complete data, consumption below violation threshold
-- Note: We calculate this directly instead of referencing violators to avoid circular dependency
compliant as (
    select
        c.meter_id,
        c.quarter,
        c.region,
        c.multiplication_factor,
        c.morning_avg_consumption * c.multiplication_factor as morning_avg_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_avg_mf,
        c.total_avg_consumption * c.multiplication_factor as total_avg_mf
    from consumption c
    inner join quality q
        on c.meter_id = q.meter_id and c.quarter = q.quarter
    -- Exclude violators: meters exceeding threshold in either period
    where NOT (
        c.morning_avg_consumption * c.multiplication_factor > {{ var('violation_threshold_watts') }}
        OR c.evening_avg_consumption * c.multiplication_factor > {{ var('violation_threshold_watts') }}
    )
),

-- Overall benchmarks (all compliant meters)
overall_benchmarks as (
    select
        'overall' as benchmark_level,
        'all' as benchmark_key,
        COUNT(*) as meter_count,

        -- Morning period statistics
        ROUND(AVG(morning_avg_mf), 3) as morning_avg,
        ROUND(STDDEV(morning_avg_mf), 3) as morning_stddev,
        ROUND(MIN(morning_avg_mf), 3) as morning_min,
        ROUND(MAX(morning_avg_mf), 3) as morning_max,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(10)], 3) as morning_p10,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(25)], 3) as morning_p25,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(50)], 3) as morning_p50,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(75)], 3) as morning_p75,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(90)], 3) as morning_p90,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(95)], 3) as morning_p95,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(99)], 3) as morning_p99,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(25)], 3) as morning_iqr,

        -- Evening period statistics
        ROUND(AVG(evening_avg_mf), 3) as evening_avg,
        ROUND(STDDEV(evening_avg_mf), 3) as evening_stddev,
        ROUND(MIN(evening_avg_mf), 3) as evening_min,
        ROUND(MAX(evening_avg_mf), 3) as evening_max,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(10)], 3) as evening_p10,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(25)], 3) as evening_p25,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(50)], 3) as evening_p50,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(75)], 3) as evening_p75,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(90)], 3) as evening_p90,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(95)], 3) as evening_p95,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(99)], 3) as evening_p99,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(25)], 3) as evening_iqr,

        -- Total period statistics
        ROUND(AVG(total_avg_mf), 3) as total_avg,
        ROUND(STDDEV(total_avg_mf), 3) as total_stddev,
        ROUND(MIN(total_avg_mf), 3) as total_min,
        ROUND(MAX(total_avg_mf), 3) as total_max,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(10)], 3) as total_p10,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(25)], 3) as total_p25,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(50)], 3) as total_p50,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(75)], 3) as total_p75,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(90)], 3) as total_p90,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(95)], 3) as total_p95,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(99)], 3) as total_p99,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(25)], 3) as total_iqr

    from compliant
),

-- Regional benchmarks
regional_benchmarks as (
    select
        'regional' as benchmark_level,
        region as benchmark_key,
        COUNT(*) as meter_count,

        -- Morning
        ROUND(AVG(morning_avg_mf), 3) as morning_avg,
        ROUND(STDDEV(morning_avg_mf), 3) as morning_stddev,
        ROUND(MIN(morning_avg_mf), 3) as morning_min,
        ROUND(MAX(morning_avg_mf), 3) as morning_max,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(10)], 3) as morning_p10,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(25)], 3) as morning_p25,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(50)], 3) as morning_p50,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(75)], 3) as morning_p75,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(90)], 3) as morning_p90,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(95)], 3) as morning_p95,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(99)], 3) as morning_p99,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(25)], 3) as morning_iqr,

        -- Evening
        ROUND(AVG(evening_avg_mf), 3) as evening_avg,
        ROUND(STDDEV(evening_avg_mf), 3) as evening_stddev,
        ROUND(MIN(evening_avg_mf), 3) as evening_min,
        ROUND(MAX(evening_avg_mf), 3) as evening_max,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(10)], 3) as evening_p10,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(25)], 3) as evening_p25,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(50)], 3) as evening_p50,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(75)], 3) as evening_p75,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(90)], 3) as evening_p90,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(95)], 3) as evening_p95,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(99)], 3) as evening_p99,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(25)], 3) as evening_iqr,

        -- Total
        ROUND(AVG(total_avg_mf), 3) as total_avg,
        ROUND(STDDEV(total_avg_mf), 3) as total_stddev,
        ROUND(MIN(total_avg_mf), 3) as total_min,
        ROUND(MAX(total_avg_mf), 3) as total_max,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(10)], 3) as total_p10,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(25)], 3) as total_p25,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(50)], 3) as total_p50,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(75)], 3) as total_p75,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(90)], 3) as total_p90,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(95)], 3) as total_p95,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(99)], 3) as total_p99,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(25)], 3) as total_iqr

    from compliant
    where region is not null
    group by region
),

-- Quarterly benchmarks (for seasonal analysis)
quarterly_benchmarks as (
    select
        'quarterly' as benchmark_level,
        quarter as benchmark_key,
        COUNT(*) as meter_count,

        -- Morning
        ROUND(AVG(morning_avg_mf), 3) as morning_avg,
        ROUND(STDDEV(morning_avg_mf), 3) as morning_stddev,
        ROUND(MIN(morning_avg_mf), 3) as morning_min,
        ROUND(MAX(morning_avg_mf), 3) as morning_max,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(10)], 3) as morning_p10,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(25)], 3) as morning_p25,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(50)], 3) as morning_p50,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(75)], 3) as morning_p75,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(90)], 3) as morning_p90,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(95)], 3) as morning_p95,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(99)], 3) as morning_p99,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(25)], 3) as morning_iqr,

        -- Evening
        ROUND(AVG(evening_avg_mf), 3) as evening_avg,
        ROUND(STDDEV(evening_avg_mf), 3) as evening_stddev,
        ROUND(MIN(evening_avg_mf), 3) as evening_min,
        ROUND(MAX(evening_avg_mf), 3) as evening_max,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(10)], 3) as evening_p10,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(25)], 3) as evening_p25,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(50)], 3) as evening_p50,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(75)], 3) as evening_p75,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(90)], 3) as evening_p90,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(95)], 3) as evening_p95,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(99)], 3) as evening_p99,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(25)], 3) as evening_iqr,

        -- Total
        ROUND(AVG(total_avg_mf), 3) as total_avg,
        ROUND(STDDEV(total_avg_mf), 3) as total_stddev,
        ROUND(MIN(total_avg_mf), 3) as total_min,
        ROUND(MAX(total_avg_mf), 3) as total_max,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(10)], 3) as total_p10,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(25)], 3) as total_p25,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(50)], 3) as total_p50,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(75)], 3) as total_p75,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(90)], 3) as total_p90,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(95)], 3) as total_p95,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(99)], 3) as total_p99,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(25)], 3) as total_iqr

    from compliant
    group by quarter
),

-- Size-based benchmarks (by multiplication factor)
size_benchmarks as (
    select
        'size_based' as benchmark_level,
        CAST(multiplication_factor AS STRING) as benchmark_key,
        COUNT(*) as meter_count,

        -- Morning
        ROUND(AVG(morning_avg_mf), 3) as morning_avg,
        ROUND(STDDEV(morning_avg_mf), 3) as morning_stddev,
        ROUND(MIN(morning_avg_mf), 3) as morning_min,
        ROUND(MAX(morning_avg_mf), 3) as morning_max,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(10)], 3) as morning_p10,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(25)], 3) as morning_p25,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(50)], 3) as morning_p50,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(75)], 3) as morning_p75,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(90)], 3) as morning_p90,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(95)], 3) as morning_p95,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(99)], 3) as morning_p99,
        ROUND(APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(morning_avg_mf, 100)[OFFSET(25)], 3) as morning_iqr,

        -- Evening
        ROUND(AVG(evening_avg_mf), 3) as evening_avg,
        ROUND(STDDEV(evening_avg_mf), 3) as evening_stddev,
        ROUND(MIN(evening_avg_mf), 3) as evening_min,
        ROUND(MAX(evening_avg_mf), 3) as evening_max,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(10)], 3) as evening_p10,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(25)], 3) as evening_p25,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(50)], 3) as evening_p50,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(75)], 3) as evening_p75,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(90)], 3) as evening_p90,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(95)], 3) as evening_p95,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(99)], 3) as evening_p99,
        ROUND(APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(evening_avg_mf, 100)[OFFSET(25)], 3) as evening_iqr,

        -- Total
        ROUND(AVG(total_avg_mf), 3) as total_avg,
        ROUND(STDDEV(total_avg_mf), 3) as total_stddev,
        ROUND(MIN(total_avg_mf), 3) as total_min,
        ROUND(MAX(total_avg_mf), 3) as total_max,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(10)], 3) as total_p10,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(25)], 3) as total_p25,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(50)], 3) as total_p50,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(75)], 3) as total_p75,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(90)], 3) as total_p90,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(95)], 3) as total_p95,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(99)], 3) as total_p99,
        ROUND(APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(75)] - APPROX_QUANTILES(total_avg_mf, 100)[OFFSET(25)], 3) as total_iqr

    from compliant
    group by multiplication_factor
)

-- Combine all benchmark levels
select * from overall_benchmarks
union all
select * from regional_benchmarks
union all
select * from quarterly_benchmarks
union all
select * from size_benchmarks
