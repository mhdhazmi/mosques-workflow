with source as (
    select * from {{ source('raw_meter_readings', 'smart_meters_clean') }}
),

renamed as (
    select
        CAST(METER_NO as STRING) as meter_id,
        DATA_TIME as reading_at,
        IMPORT_ACTIVE_POWER as active_power_watts,
        
        -- Extract date and time components for easier filtering
        DATE(DATA_TIME) as reading_date,
        TIME(DATA_TIME) as reading_time

    from source
)

select * from renamed
