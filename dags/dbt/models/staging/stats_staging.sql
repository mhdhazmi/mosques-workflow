{{ config(
    materialized='incremental',
    unique_key='run_id',
    on_schema_change='append_new_columns'
) }}

-- Collect stats from the staging layer transformation
-- Tracks deduplication and outlier filtering effects

{% set run_id = var('pipeline_run_id', 'unknown') %}
{% set run_timestamp = run_started_at %}

with source_counts as (
    -- Count raw rows from BigQuery source table
    select
        count(*) as rows_raw
    from {{ source('raw_meter_readings', 'smart_meters_clean') }}
),

staged_counts as (
    -- Count rows after staging transformations (dedup + outlier filtering)
    select
        count(*) as rows_after_staging,
        count(distinct meter_id) as unique_meters,
        min(reading_date) as min_reading_date,
        max(reading_date) as max_reading_date,
        sum(case when active_power_watts is null then 1 else 0 end) as rows_with_null_power,
        sum(case when active_power_watts = 0 then 1 else 0 end) as rows_with_zero_power
    from {{ ref('stg_meter_readings') }}
)

select
    '{{ run_id }}' as run_id,
    timestamp('{{ run_timestamp }}') as run_timestamp,
    'all_files' as source_filename,
    'staging' as stage_name,
    sc.rows_raw as rows_input,
    stg.rows_after_staging as rows_output,
    sc.rows_raw - stg.rows_after_staging as rows_filtered,
    'dedup_and_outliers' as filter_reason,
    stg.unique_meters,
    stg.min_reading_date,
    stg.max_reading_date,
    stg.rows_with_null_power,
    stg.rows_with_zero_power,
    'success' as status
from source_counts sc
cross join staged_counts stg

{% if is_incremental() %}
where '{{ run_id }}' not in (select run_id from {{ this }})
{% endif %}

