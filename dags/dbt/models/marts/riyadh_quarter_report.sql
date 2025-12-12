{{ config(
    materialized='table'
) }}

-- Riyadh Quarter Report
-- Matches the team's report format for comparison
-- Aggregates consumption and over-consumer statistics for Riyadh meters

with quality_before_filter as (
    -- Get total Riyadh meters BEFORE quality filter
    select
        COUNT(DISTINCT q.meter_id) as total_meters_before_quality_filter
    from {{ ref('int_meter_quality') }} q
    inner join {{ ref('int_meter_locations') }} m 
        on CAST(q.meter_id as STRING) = CAST(m.meter_id as STRING)
    where m.region = 'Central' and m.province like '%RIYADH%'
),

riyadh_data as (
    -- All Riyadh meters after quality filter (both violators and compliant)
    select * from {{ ref('consumption_riyadh') }}
),

meter_stats as (
    select
        COUNT(DISTINCT meter_id) as total_meters_after_quality_filter,
        COUNTIF(over_in_morning = TRUE) as overs_morning,
        COUNTIF(over_in_evening = TRUE) as overs_evening,
        COUNTIF(over_in_both = TRUE) as overs_both,
        COUNTIF(over_in_either = TRUE) as overs_either
    from riyadh_data
),

consumption_by_category as (
    -- Calculate total consumption for over-consumers vs regular
    select
        -- Over consumers (flagged in at least one period)
        SUM(CASE WHEN over_in_morning = TRUE THEN morning_energy_kwh ELSE 0 END) +
        SUM(CASE WHEN over_in_evening = TRUE THEN evening_energy_kwh ELSE 0 END) as over_consumers_total_kwh,
        
        SUM(CASE WHEN over_in_morning = TRUE THEN morning_cost_sar ELSE 0 END) +
        SUM(CASE WHEN over_in_evening = TRUE THEN evening_cost_sar ELSE 0 END) as over_consumers_total_cost_sar,
        
        -- Regular consumers (compliant - NOT flagged in that specific period)
        SUM(CASE WHEN over_in_morning = FALSE THEN morning_energy_kwh ELSE 0 END) +
        SUM(CASE WHEN over_in_evening = FALSE THEN evening_energy_kwh ELSE 0 END) as regular_consumers_total_kwh,
        
        SUM(CASE WHEN over_in_morning = FALSE THEN morning_cost_sar ELSE 0 END) +
        SUM(CASE WHEN over_in_evening = FALSE THEN evening_cost_sar ELSE 0 END) as regular_consumers_total_cost_sar
    from riyadh_data
)

select
    -- Meter counts
    q.total_meters_before_quality_filter,
    m.total_meters_after_quality_filter,
    
    -- Over-consumer counts
    m.overs_morning,
    m.overs_evening,
    m.overs_both,
    m.overs_either,
    
    -- Over consumers energy & cost
    ROUND(c.over_consumers_total_kwh / 1000000, 2) as over_consumers_gwh,
    ROUND(c.over_consumers_total_cost_sar / 1000000, 2) as over_consumers_cost_million_sar,
    
    -- Regular consumers energy & cost
    ROUND(c.regular_consumers_total_kwh / 1000000, 2) as regular_consumers_gwh,
    ROUND(c.regular_consumers_total_cost_sar / 1000000, 2) as regular_consumers_cost_million_sar,
    
    -- Comparison reference (team's numbers)
    14094 as team_total_meters_before,
    12673 as team_total_meters_after,
    6591 as team_overs_morning,
    7027 as team_overs_evening,
    5959 as team_overs_both,
    7659 as team_overs_either,
    60.34 as team_over_consumers_gwh,
    19.31 as team_over_consumers_cost_million_sar,
    6.42 as team_regular_consumers_gwh,
    2.06 as team_regular_consumers_cost_million_sar

from quality_before_filter q
cross join meter_stats m
cross join consumption_by_category c

