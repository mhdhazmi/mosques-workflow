with meters as (
    select * from {{ ref('stg_industry_codes') }}
),

prayer_locations as (
    -- Get unique locations from prayer times
    select
        ANY_VALUE(location) as location,
        original_coordinate
    from {{ ref('stg_prayer_times') }}
    group by original_coordinate
),

joined as (
    select
        m.meter_id,
        m.multiplication_factor,
        m.location as meter_location,
        m.region,
        m.province,
        
        -- Find nearest prayer location
        -- Cross join is expensive but necessary if no common key. 
        -- Optimization: Filter by distance if possible, or use BQ GEOGRAPHY functions.
        -- For now, we use a cross join and rank by distance.
        p.original_coordinate as nearest_prayer_coordinate,
        ST_DISTANCE(m.location, p.location) as distance_meters

    from meters m
    cross join prayer_locations p
),

ranked as (
    select 
        *,
        ROW_NUMBER() OVER (PARTITION BY meter_id ORDER BY distance_meters ASC) as rn
    from joined
)

select * from ranked where rn = 1
