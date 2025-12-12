{{ config(
    materialized='incremental',
    unique_key='run_id',
    on_schema_change='append_new_columns'
) }}

-- Collect stats from the Ramadan exclusion transformation
-- Tracks how many rows were filtered due to Ramadan dates

{% set run_id = var('pipeline_run_id', 'unknown') %}
{% set run_timestamp = run_started_at %}
{% set ramadan_start = var('ramadan_start', '1900-01-01') %}
{% set ramadan_end = var('ramadan_end', '1900-01-01') %}

with before_ramadan as (
    -- Count rows before Ramadan filter
    select
        count(*) as rows_before,
        count(distinct meter_id) as unique_meters_before
    from {{ ref('stg_meter_readings') }}
),

after_ramadan as (
    -- Count rows after Ramadan filter
    select
        count(*) as rows_after,
        count(distinct meter_id) as unique_meters_after,
        min(reading_date) as min_reading_date,
        max(reading_date) as max_reading_date
    from {{ ref('int_exclude_ramadan') }}
),

ramadan_excluded as (
    -- Count rows that fell within Ramadan dates (if filter is active)
    select
        count(*) as rows_in_ramadan
    from {{ ref('stg_meter_readings') }}
    {% if ramadan_start != '1900-01-01' and ramadan_end != '1900-01-01' %}
    where reading_date between '{{ ramadan_start }}' and '{{ ramadan_end }}'
    {% else %}
    where false  -- No Ramadan filter active, so no rows excluded
    {% endif %}
)

select
    '{{ run_id }}' as run_id,
    timestamp('{{ run_timestamp }}') as run_timestamp,
    'all_files' as source_filename,
    'ramadan_filter' as stage_name,
    br.rows_before as rows_input,
    ar.rows_after as rows_output,
    re.rows_in_ramadan as rows_filtered,
    case 
        when re.rows_in_ramadan > 0 then 'ramadan_dates_{{ ramadan_start }}_to_{{ ramadan_end }}'
        else 'no_ramadan_filter_active'
    end as filter_reason,
    ar.unique_meters_after as unique_meters,
    ar.min_reading_date,
    ar.max_reading_date,
    'success' as status
from before_ramadan br
cross join after_ramadan ar
cross join ramadan_excluded re

{% if is_incremental() %}
where '{{ run_id }}' not in (select run_id from {{ this }})
{% endif %}

