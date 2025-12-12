with source as (
    select * from {{ source('raw_meter_readings', 'smart_meters_clean') }}
),

deduplicated as (
    -- Remove duplicates using ROW_HASH (METER_ID + DATA_TIME) created by etl_processor.py.
    -- This ensures deduplication logic is identical to BigQuery MERGE in cloud_loader.py.
    select * from source
    qualify ROW_NUMBER() OVER (
        PARTITION BY ROW_HASH
        ORDER BY DATA_TIME DESC
    ) = 1
),

renamed as (
    select
        CAST(METER_ID as STRING) as meter_id,
        DATA_TIME as reading_at,

        -- Filter outliers: Set to NULL if > 1GW (1,000,000,000 watts)
        -- Normal residential/commercial meters should be < 1MW
        -- Round to 3 decimal places for consistent precision
        ROUND(
            CASE
                WHEN IMPORT_ACTIVE_POWER > 1000000000 THEN NULL
                WHEN IMPORT_ACTIVE_POWER < 0 THEN NULL
                ELSE IMPORT_ACTIVE_POWER
            END,
            3
        ) as active_power_watts,

        -- Extract date and time components for easier filtering
        DATE(DATA_TIME) as reading_date,
        TIME(DATA_TIME) as reading_time,

        -- Quarter: Try to use from source, otherwise compute from DATA_TIME
        CAST(EXTRACT(YEAR FROM DATA_TIME) as STRING) || '-Q' || CAST(EXTRACT(QUARTER FROM DATA_TIME) as STRING) as quarter

    from deduplicated
)

select * from renamed
