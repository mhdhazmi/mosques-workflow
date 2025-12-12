-- models/marts/consumption_stats.sql
-- Uses stg_meter_readings to ensure:
--   - Sentinel values (>1GW) are filtered out
--   - Duplicates are removed
--   - Consistent data lineage with other models

{{ config(
    materialized='incremental',
    unique_key=['meter_id', 'date', 'quarter'],
    on_schema_change='append_new_columns',
    partition_by={
      "field": "date",
      "data_type": "date",
      "granularity": "day"
    }
) }}

WITH staged_data AS (
    -- Use int_exclude_ramadan to filter out Ramadan dates when configured
    SELECT
        meter_id,
        reading_date as date,
        quarter,
        active_power_watts as power_watts
    FROM {{ ref('int_exclude_ramadan') }}
    WHERE active_power_watts IS NOT NULL  -- Exclude filtered outliers
    {% if is_incremental() %}
        -- Only process new data: get max date from existing table
        AND reading_date > (SELECT COALESCE(MAX(date), '1900-01-01') FROM {{ this }})
    {% endif %}
)

SELECT
    meter_id,
    date,
    quarter,
    AVG(power_watts) as avg_daily_power_watts,
    MAX(power_watts) as max_daily_power_watts,
    MIN(power_watts) as min_daily_power_watts,
    COUNT(*) as readings_count
FROM staged_data
GROUP BY meter_id, date, quarter
