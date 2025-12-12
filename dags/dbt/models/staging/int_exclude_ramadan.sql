{{ config(
    materialized='view'
) }}

-- Exclude Ramadan dates from meter readings
-- Ramadan dates vary each year, configure in dbt_project.yml:
--   vars:
--     ramadan_start: '2025-02-28'
--     ramadan_end: '2025-03-30'
--
-- For Q3 2025 (Jul-Sep), Ramadan was in Q1 (Feb-Mar), so no exclusion needed.
-- This model is ready for future quarters that overlap with Ramadan.

{% set ramadan_start = var('ramadan_start', '1900-01-01') %}
{% set ramadan_end = var('ramadan_end', '1900-01-01') %}

with staged as (
    select * from {{ ref('stg_meter_readings') }}
),

filtered as (
    select *
    from staged
    {% if ramadan_start != '1900-01-01' and ramadan_end != '1900-01-01' %}
    where reading_date NOT BETWEEN '{{ ramadan_start }}' AND '{{ ramadan_end }}'
    {% endif %}
)

select * from filtered

