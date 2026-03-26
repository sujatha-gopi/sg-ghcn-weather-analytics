/*
    fact_weather_daily.sql
    ----------------------
    Final analytical table joining cleaned measurements
    with station metadata.

    This is the table Looker Studio will query directly.
    Materialized as a TABLE (not view) for dashboard performance.

    Grain: one row per station + date + element
*/

with measurements as (
    select * from {{ ref('stg_ghcn_measurements') }}
),

stations as (
    select * from {{ ref('stg_ghcn_stations') }}
),

joined as (
    select
        -- Time dimensions
        m.measurement_date,
        m.measurement_year,
        m.measurement_month,

        -- Location dimensions
        m.country_code,
        s.country_name,
        s.continent,
        m.station_id,
        s.station_name,
        s.latitude,
        s.longitude,
        s.elevation_m,

        -- Measurement
        m.element,
        m.measurement_value,
        m.unit

    from measurements m
    left join stations s
        on m.station_id = s.station_id
    where
        -- Only keep rows where we have station metadata
        s.station_id is not null
)

select * from joined
