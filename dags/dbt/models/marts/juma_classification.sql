{{
    config(
        materialized='table'
    )
}}

-- Juma Mosque Classification
-- Classifies mosques as Juma (large Friday prayer mosques) or Regular
-- based on Friday Dhuhr consumption patterns vs weekday Dhuhr consumption

with readings_with_periods as (
    select * from {{ ref('int_meter_readings_with_periods') }}
),

industry_codes as (
    select
        meter_id,
        multiplication_factor
    from {{ ref('stg_industry_codes') }}
),

-- Calculate Dhuhr period consumption separately for Fridays and weekdays
dhuhr_consumption as (
    select
        r.meter_id,
        r.quarter,

        -- Friday Dhuhr consumption (DAYOFWEEK 6 = Friday in BigQuery)
        AVG(CASE
            WHEN EXTRACT(DAYOFWEEK FROM r.reading_date) = 6
                AND r.reading_time >= r.dhuhr_time
                AND r.reading_time <= TIME_ADD(r.dhuhr_time, INTERVAL 90 MINUTE)
            THEN r.active_power_watts
        END) as friday_dhuhr_avg,

        COUNT(CASE
            WHEN EXTRACT(DAYOFWEEK FROM r.reading_date) = 6
                AND r.reading_time >= r.dhuhr_time
                AND r.reading_time <= TIME_ADD(r.dhuhr_time, INTERVAL 90 MINUTE)
                AND r.active_power_watts IS NOT NULL
            THEN 1
        END) as friday_reading_count,

        -- Weekday Dhuhr consumption (all days except Friday)
        AVG(CASE
            WHEN EXTRACT(DAYOFWEEK FROM r.reading_date) != 6
                AND r.reading_time >= r.dhuhr_time
                AND r.reading_time <= TIME_ADD(r.dhuhr_time, INTERVAL 90 MINUTE)
            THEN r.active_power_watts
        END) as weekday_dhuhr_avg,

        COUNT(CASE
            WHEN EXTRACT(DAYOFWEEK FROM r.reading_date) != 6
                AND r.reading_time >= r.dhuhr_time
                AND r.reading_time <= TIME_ADD(r.dhuhr_time, INTERVAL 90 MINUTE)
                AND r.active_power_watts IS NOT NULL
            THEN 1
        END) as weekday_reading_count

    from readings_with_periods r
    where r.dhuhr_time is not null
    group by r.meter_id, r.quarter
),

-- Join with industry codes to get multiplication factor
with_metadata as (
    select
        d.meter_id,
        d.quarter,
        d.friday_dhuhr_avg,
        d.weekday_dhuhr_avg,
        d.friday_reading_count,
        d.weekday_reading_count,
        COALESCE(i.multiplication_factor, 1.0) as multiplication_factor
    from dhuhr_consumption d
    left join industry_codes i on d.meter_id = i.meter_id
),

-- Calculate ratio and classify
classified as (
    select
        meter_id,
        quarter,
        ROUND(friday_dhuhr_avg, 3) as friday_dhuhr_avg,
        ROUND(weekday_dhuhr_avg, 3) as weekday_dhuhr_avg,
        friday_reading_count,
        weekday_reading_count,
        multiplication_factor,

        -- Calculate Friday to weekday ratio (handle division by zero)
        CASE
            WHEN weekday_dhuhr_avg IS NULL OR weekday_dhuhr_avg = 0 THEN NULL
            ELSE ROUND(friday_dhuhr_avg / weekday_dhuhr_avg, 3)
        END as friday_weekday_ratio,

        -- Data quality check
        CASE
            WHEN friday_reading_count >= 4 AND weekday_reading_count >= 20 THEN TRUE
            ELSE FALSE
        END as has_sufficient_data,

        -- Juma classification based on configurable threshold
        CASE
            WHEN friday_reading_count < 4 OR weekday_reading_count < 20 THEN FALSE
            WHEN weekday_dhuhr_avg IS NULL OR weekday_dhuhr_avg = 0 THEN FALSE
            WHEN (friday_dhuhr_avg / weekday_dhuhr_avg) >= {{ var('juma_ratio_threshold') }} THEN TRUE
            -- Also classify as Juma if very large facility (MF >= 100) even with lower ratio
            WHEN multiplication_factor >= 100 AND (friday_dhuhr_avg / weekday_dhuhr_avg) >= 1.2 THEN TRUE
            ELSE FALSE
        END as is_juma,

        -- Confidence scoring based on ratio and multiplication factor
        CASE
            WHEN friday_reading_count < 4 OR weekday_reading_count < 20 THEN 'INSUFFICIENT_DATA'
            WHEN weekday_dhuhr_avg IS NULL OR weekday_dhuhr_avg = 0 THEN 'NO_BASELINE'
            -- High confidence: meets threshold AND large facility
            WHEN (friday_dhuhr_avg / weekday_dhuhr_avg) >= {{ var('juma_ratio_threshold') }}
                AND multiplication_factor >= 40 THEN 'HIGH'
            -- Medium confidence: meets threshold but smaller facility
            WHEN (friday_dhuhr_avg / weekday_dhuhr_avg) >= {{ var('juma_ratio_threshold') }}
                AND multiplication_factor < 40 THEN 'MEDIUM'
            -- Medium confidence: large facility suggests Juma despite lower ratio
            WHEN multiplication_factor >= 100
                AND (friday_dhuhr_avg / weekday_dhuhr_avg) >= 1.2 THEN 'MEDIUM'
            -- Low confidence: doesn't meet criteria
            ELSE 'LOW'
        END as juma_confidence,

        -- Classification reason for transparency
        CASE
            WHEN friday_reading_count < 4 OR weekday_reading_count < 20
                THEN 'Insufficient readings for classification'
            WHEN weekday_dhuhr_avg IS NULL OR weekday_dhuhr_avg = 0
                THEN 'No weekday baseline available'
            WHEN (friday_dhuhr_avg / weekday_dhuhr_avg) >= {{ var('juma_ratio_threshold') }}
                AND multiplication_factor >= 40
                THEN 'High Friday spike with large facility - likely Juma mosque'
            WHEN (friday_dhuhr_avg / weekday_dhuhr_avg) >= {{ var('juma_ratio_threshold') }}
                AND multiplication_factor < 40
                THEN 'High Friday spike - possible Juma mosque'
            WHEN multiplication_factor >= 100
                AND (friday_dhuhr_avg / weekday_dhuhr_avg) >= 1.2
                THEN 'Very large facility with moderate Friday increase - likely Juma mosque'
            WHEN multiplication_factor >= 100
                THEN 'Large facility but no significant Friday spike - likely regular mosque'
            ELSE 'No significant Friday consumption pattern - regular mosque'
        END as classification_reason

    from with_metadata
)

select * from classified
