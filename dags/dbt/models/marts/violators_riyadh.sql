{{ config(
    materialized='table'
) }}

-- Riyadh-specific violators report
-- Only over-consumers (meters exceeding 3000W threshold in at least one period)
-- This matches the team's pipeline step 9: "get Riyadh mosques only"

with riyadh_data as (
    select * from {{ ref('consumption_riyadh') }}
    where over_in_either = TRUE  -- Only violators
)

select
    *,
    
    -- Summary flags for reporting (window functions for aggregates)
    COUNTIF(over_in_morning) OVER () as total_overs_morning,
    COUNTIF(over_in_evening) OVER () as total_overs_evening,
    COUNTIF(over_in_both) OVER () as total_overs_both,
    COUNTIF(over_in_either) OVER () as total_overs_either,
    COUNT(*) OVER () as total_violators_riyadh

from riyadh_data

