{{ config(
    materialized='table'
) }}

with period_data as (
    select * from {{ ref('int_meter_readings_with_periods') }}
),

meter_info as (
    select * from {{ ref('int_meter_locations') }}
),

aggregated as (
    select
        p.meter_id,
        
        -- Morning Stats (Exclude Fridays)
        AVG(CASE WHEN 
                (p.reading_time BETWEEN p.morning_start_time AND p.morning_end_time) 
                AND EXTRACT(DAYOFWEEK FROM p.reading_date) != 6 
            THEN p.active_power_watts END) as morning_avg_consumption,
            
        SUM(CASE WHEN 
                (p.reading_time BETWEEN p.morning_start_time AND p.morning_end_time) 
                AND EXTRACT(DAYOFWEEK FROM p.reading_date) != 6
            THEN p.active_power_watts END) as morning_sum_consumption,

        -- Evening Stats
        AVG(CASE WHEN 
                (p.reading_time >= p.evening_start_time OR p.reading_time <= p.evening_end_time)
            THEN p.active_power_watts END) as evening_avg_consumption,

        SUM(CASE WHEN 
                (p.reading_time >= p.evening_start_time OR p.reading_time <= p.evening_end_time)
            THEN p.active_power_watts END) as evening_sum_consumption,

        -- Debug Columns
        MIN(p.reading_date) as min_reading_date,
        MAX(p.reading_date) as max_reading_date,
        COUNT(p.morning_start_time) as readings_with_prayer_times

    from period_data p
    group by p.meter_id
)

select
    a.*,
    m.multiplication_factor,
    m.region,
    m.province,
    
    -- Apply Multiplication Factor (MF)
    -- Note: Sum is in Watts per 30 mins (assuming 30 min intervals). 
    -- To get kWh: (Sum * MF) / 1000 / 2 (since 2 readings per hour)
    -- Notebook logic: merged_df['Morning_Sum_Consume_MF'] = merged_df['Morning_Sum_Consume'] * merged_df['Multiplication Factor'] / 2
    
    (a.morning_sum_consumption * m.multiplication_factor / 2) / 1000 as morning_energy_kwh,
    (a.evening_sum_consumption * m.multiplication_factor / 2) / 1000 as evening_energy_kwh,
    
    -- Calculate Cost (0.32 SAR/kWh)
    ((a.morning_sum_consumption * m.multiplication_factor / 2) / 1000) * 0.32 as morning_cost_sar,
    ((a.evening_sum_consumption * m.multiplication_factor / 2) / 1000) * 0.32 as evening_cost_sar

from aggregated a
left join meter_info m on CAST(a.meter_id as STRING) = CAST(m.meter_id as STRING)
