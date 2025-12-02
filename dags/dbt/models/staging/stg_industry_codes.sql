with source as (
    select * from {{ ref('industry_codes') }}
),

renamed as (
    select
        CAST(`Meter Number` as STRING) as meter_id,
        -- Handle '#' in Multiplication Factor by defaulting to 1.0
        COALESCE(SAFE_CAST(NULLIF(TRIM(`Multiplication Factor`), '#') as FLOAT64), 1.0) as multiplication_factor,
        
        -- Create Geography Point from X (Lon) and Y (Lat)
        ST_GEOGPOINT(
            SAFE_CAST(`X Coordinates` as FLOAT64),
            SAFE_CAST(`Y Coordinates` as FLOAT64)
        ) as location,
        
        `Region` as region,
        `Province` as province,
        `Department Name` as department_name,
        `Office Name` as office_name

    from source
)

select * from renamed
