-- =============================================================================
-- NORMAL MOSQUE CONSUMPTION PATTERN ANALYSIS
-- =============================================================================
-- Run these queries in BigQuery Console to analyze normal consumption patterns
-- for mosques, excluding violators (over-consumers)
-- =============================================================================

-- =============================================================================
-- QUERY 1: Get Compliant Meter Count vs Violator Count
-- =============================================================================
SELECT
    'Total Meters' as category,
    COUNT(DISTINCT meter_id) as meter_count
FROM `raw_meter_readings.consumption_analysis`
WHERE data_quality_flag = 'COMPLETE'

UNION ALL

SELECT
    'Violators (>3000W)',
    COUNT(DISTINCT meter_id)
FROM `raw_meter_readings.violators`

UNION ALL

SELECT
    'Compliant (<3000W)',
    COUNT(DISTINCT c.meter_id)
FROM `raw_meter_readings.consumption_analysis` c
LEFT JOIN `raw_meter_readings.violators` v
    ON c.meter_id = v.meter_id AND c.quarter = v.quarter
WHERE v.meter_id IS NULL
  AND c.data_quality_flag = 'COMPLETE';


-- =============================================================================
-- QUERY 2: Consumption Statistics for Compliant Meters
-- =============================================================================
WITH compliant AS (
    SELECT
        c.meter_id,
        c.quarter,
        c.morning_avg_consumption,
        c.evening_avg_consumption,
        c.total_avg_consumption,
        c.multiplication_factor,
        c.morning_avg_consumption * c.multiplication_factor as morning_with_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_with_mf,
        c.total_avg_consumption * c.multiplication_factor as total_with_mf,
        c.region
    FROM `raw_meter_readings.consumption_analysis` c
    INNER JOIN `raw_meter_readings.int_meter_quality` q
        ON c.meter_id = q.meter_id AND c.quarter = q.quarter
    LEFT JOIN `raw_meter_readings.violators` v
        ON c.meter_id = v.meter_id AND c.quarter = v.quarter
    WHERE v.meter_id IS NULL
      AND q.is_good_quality = TRUE
      AND c.data_quality_flag = 'COMPLETE'
)
SELECT
    'Morning (with MF)' as period,
    COUNT(*) as count,
    ROUND(AVG(morning_with_mf), 2) as mean_watts,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(50)], 2) as median_watts,
    ROUND(STDDEV(morning_with_mf), 2) as std_watts,
    ROUND(MIN(morning_with_mf), 2) as min_watts,
    ROUND(MAX(morning_with_mf), 2) as max_watts
FROM compliant
WHERE morning_with_mf IS NOT NULL

UNION ALL

SELECT
    'Evening (with MF)',
    COUNT(*),
    ROUND(AVG(evening_with_mf), 2),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(50)], 2),
    ROUND(STDDEV(evening_with_mf), 2),
    ROUND(MIN(evening_with_mf), 2),
    ROUND(MAX(evening_with_mf), 2)
FROM compliant
WHERE evening_with_mf IS NOT NULL

UNION ALL

SELECT
    'Total (with MF)',
    COUNT(*),
    ROUND(AVG(total_with_mf), 2),
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(50)], 2),
    ROUND(STDDEV(total_with_mf), 2),
    ROUND(MIN(total_with_mf), 2),
    ROUND(MAX(total_with_mf), 2)
FROM compliant
WHERE total_with_mf IS NOT NULL;


-- =============================================================================
-- QUERY 3: Percentile Distribution (Normal Range Definition)
-- =============================================================================
WITH compliant AS (
    SELECT
        c.morning_avg_consumption * c.multiplication_factor as morning_with_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_with_mf,
        c.total_avg_consumption * c.multiplication_factor as total_with_mf
    FROM `raw_meter_readings.consumption_analysis` c
    INNER JOIN `raw_meter_readings.int_meter_quality` q
        ON c.meter_id = q.meter_id AND c.quarter = q.quarter
    LEFT JOIN `raw_meter_readings.violators` v
        ON c.meter_id = v.meter_id AND c.quarter = v.quarter
    WHERE v.meter_id IS NULL
      AND q.is_good_quality = TRUE
      AND c.data_quality_flag = 'COMPLETE'
)
SELECT
    'Morning Period' as period,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(5)], 0) as P05,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(10)], 0) as P10,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(25)], 0) as P25,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(50)], 0) as P50_Median,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(75)], 0) as P75,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(90)], 0) as P90,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(95)], 0) as P95,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(99)], 0) as P99
FROM compliant

UNION ALL

SELECT
    'Evening Period',
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(5)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(10)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(25)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(50)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(75)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(90)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(95)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(99)], 0)
FROM compliant

UNION ALL

SELECT
    'Total Consumption',
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(5)], 0),
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(10)], 0),
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(25)], 0),
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(50)], 0),
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(75)], 0),
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(90)], 0),
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(95)], 0),
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(99)], 0)
FROM compliant;


-- =============================================================================
-- QUERY 4: Regional Consumption Patterns
-- =============================================================================
WITH compliant AS (
    SELECT
        c.meter_id,
        c.region,
        c.morning_avg_consumption * c.multiplication_factor as morning_with_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_with_mf,
        c.total_avg_consumption * c.multiplication_factor as total_with_mf
    FROM `raw_meter_readings.consumption_analysis` c
    INNER JOIN `raw_meter_readings.int_meter_quality` q
        ON c.meter_id = q.meter_id AND c.quarter = q.quarter
    LEFT JOIN `raw_meter_readings.violators` v
        ON c.meter_id = v.meter_id AND c.quarter = v.quarter
    WHERE v.meter_id IS NULL
      AND q.is_good_quality = TRUE
      AND c.data_quality_flag = 'COMPLETE'
)
SELECT
    region,
    COUNT(*) as meter_count,
    ROUND(AVG(morning_with_mf), 2) as morning_avg,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(50)], 2) as morning_median,
    ROUND(AVG(evening_with_mf), 2) as evening_avg,
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(50)], 2) as evening_median,
    ROUND(AVG(total_with_mf), 2) as total_avg,
    ROUND(APPROX_QUANTILES(total_with_mf, 100)[OFFSET(50)], 2) as total_median
FROM compliant
GROUP BY region
ORDER BY meter_count DESC;


-- =============================================================================
-- QUERY 5: Size-Based Patterns (by Multiplication Factor)
-- =============================================================================
WITH compliant AS (
    SELECT
        c.multiplication_factor,
        c.morning_avg_consumption,
        c.evening_avg_consumption,
        c.morning_avg_consumption * c.multiplication_factor as morning_with_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_with_mf
    FROM `raw_meter_readings.consumption_analysis` c
    INNER JOIN `raw_meter_readings.int_meter_quality` q
        ON c.meter_id = q.meter_id AND c.quarter = q.quarter
    LEFT JOIN `raw_meter_readings.violators` v
        ON c.meter_id = v.meter_id AND c.quarter = v.quarter
    WHERE v.meter_id IS NULL
      AND q.is_good_quality = TRUE
      AND c.data_quality_flag = 'COMPLETE'
)
SELECT
    multiplication_factor,
    COUNT(*) as meter_count,
    ROUND(AVG(morning_avg_consumption), 2) as raw_morning_avg,
    ROUND(AVG(morning_with_mf), 2) as adj_morning_avg,
    ROUND(AVG(evening_avg_consumption), 2) as raw_evening_avg,
    ROUND(AVG(evening_with_mf), 2) as adj_evening_avg
FROM compliant
GROUP BY multiplication_factor
ORDER BY multiplication_factor;


-- =============================================================================
-- QUERY 6: Quarterly Patterns (Seasonal)
-- =============================================================================
WITH compliant AS (
    SELECT
        c.quarter,
        c.morning_avg_consumption * c.multiplication_factor as morning_with_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_with_mf
    FROM `raw_meter_readings.consumption_analysis` c
    INNER JOIN `raw_meter_readings.int_meter_quality` q
        ON c.meter_id = q.meter_id AND c.quarter = q.quarter
    LEFT JOIN `raw_meter_readings.violators` v
        ON c.meter_id = v.meter_id AND c.quarter = v.quarter
    WHERE v.meter_id IS NULL
      AND q.is_good_quality = TRUE
      AND c.data_quality_flag = 'COMPLETE'
)
SELECT
    quarter,
    COUNT(*) as meter_count,
    ROUND(AVG(morning_with_mf), 2) as morning_avg,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(50)], 2) as morning_median,
    ROUND(STDDEV(morning_with_mf), 2) as morning_std,
    ROUND(AVG(evening_with_mf), 2) as evening_avg,
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(50)], 2) as evening_median,
    ROUND(STDDEV(evening_with_mf), 2) as evening_std
FROM compliant
GROUP BY quarter
ORDER BY quarter DESC;


-- =============================================================================
-- QUERY 7: Evening-to-Morning Ratio
-- =============================================================================
WITH compliant AS (
    SELECT
        c.morning_avg_consumption * c.multiplication_factor as morning_with_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_with_mf
    FROM `raw_meter_readings.consumption_analysis` c
    INNER JOIN `raw_meter_readings.int_meter_quality` q
        ON c.meter_id = q.meter_id AND c.quarter = q.quarter
    LEFT JOIN `raw_meter_readings.violators` v
        ON c.meter_id = v.meter_id AND c.quarter = v.quarter
    WHERE v.meter_id IS NULL
      AND q.is_good_quality = TRUE
      AND c.data_quality_flag = 'COMPLETE'
      AND c.morning_avg_consumption > 0
),
ratios AS (
    SELECT
        evening_with_mf / morning_with_mf as evening_morning_ratio
    FROM compliant
    WHERE morning_with_mf > 0
)
SELECT
    'Evening/Morning Ratio' as metric,
    ROUND(AVG(evening_morning_ratio), 3) as mean,
    ROUND(APPROX_QUANTILES(evening_morning_ratio, 100)[OFFSET(50)], 3) as median,
    ROUND(STDDEV(evening_morning_ratio), 3) as std,
    ROUND(APPROX_QUANTILES(evening_morning_ratio, 100)[OFFSET(25)], 3) as P25,
    ROUND(APPROX_QUANTILES(evening_morning_ratio, 100)[OFFSET(75)], 3) as P75
FROM ratios
WHERE evening_morning_ratio < 10;  -- Exclude extreme outliers


-- =============================================================================
-- QUERY 8: Violator vs Compliant Comparison
-- =============================================================================
SELECT
    'Compliant (Normal)' as category,
    COUNT(*) as meter_count,
    ROUND(AVG(c.morning_avg_consumption * c.multiplication_factor), 2) as morning_avg_watts,
    ROUND(AVG(c.evening_avg_consumption * c.multiplication_factor), 2) as evening_avg_watts,
    ROUND(AVG(c.total_avg_consumption * c.multiplication_factor), 2) as total_avg_watts
FROM `raw_meter_readings.consumption_analysis` c
INNER JOIN `raw_meter_readings.int_meter_quality` q
    ON c.meter_id = q.meter_id AND c.quarter = q.quarter
LEFT JOIN `raw_meter_readings.violators` v
    ON c.meter_id = v.meter_id AND c.quarter = v.quarter
WHERE v.meter_id IS NULL
  AND q.is_good_quality = TRUE
  AND c.data_quality_flag = 'COMPLETE'

UNION ALL

SELECT
    'Violators (Over-Consumer)',
    COUNT(*),
    ROUND(AVG(morning_avg_mf), 2),
    ROUND(AVG(evening_avg_mf), 2),
    ROUND((AVG(morning_avg_mf) + AVG(evening_avg_mf)) / 2, 2)
FROM `raw_meter_readings.violators`;


-- =============================================================================
-- QUERY 9: Define Normal Threshold Ranges
-- =============================================================================
-- This query outputs recommended thresholds based on percentile analysis
WITH compliant AS (
    SELECT
        c.morning_avg_consumption * c.multiplication_factor as morning_with_mf,
        c.evening_avg_consumption * c.multiplication_factor as evening_with_mf
    FROM `raw_meter_readings.consumption_analysis` c
    INNER JOIN `raw_meter_readings.int_meter_quality` q
        ON c.meter_id = q.meter_id AND c.quarter = q.quarter
    LEFT JOIN `raw_meter_readings.violators` v
        ON c.meter_id = v.meter_id AND c.quarter = v.quarter
    WHERE v.meter_id IS NULL
      AND q.is_good_quality = TRUE
      AND c.data_quality_flag = 'COMPLETE'
)
SELECT
    'Morning Normal Range' as threshold_type,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(25)], 0) as lower_bound_p25,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(75)], 0) as upper_bound_p75,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(90)], 0) as extended_upper_p90,
    ROUND(APPROX_QUANTILES(morning_with_mf, 100)[OFFSET(95)], 0) as warning_threshold_p95,
    3000 as current_violation_threshold
FROM compliant

UNION ALL

SELECT
    'Evening Normal Range',
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(25)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(75)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(90)], 0),
    ROUND(APPROX_QUANTILES(evening_with_mf, 100)[OFFSET(95)], 0),
    3000
FROM compliant;
