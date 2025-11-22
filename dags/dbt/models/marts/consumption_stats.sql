-- models/marts/consumption_stats.sql

{{ config(
    materialized='table',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

WITH raw_data AS (
    SELECT
        METER_ID,
        DATE(DATA_TIME) as date,
        IMPORT_ACTIVE_POWER as power_watts
    FROM {{ source('raw_meter_readings', 'smart_meters_clean') }}
)

SELECT
    METER_ID,
    date,
    AVG(power_watts) as avg_daily_power_watts,
    MAX(power_watts) as max_daily_power_watts,
    MIN(power_watts) as min_daily_power_watts,
    COUNT(*) as readings_count
FROM raw_data
GROUP BY 1, 2
