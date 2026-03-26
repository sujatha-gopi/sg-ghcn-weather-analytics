/*
    stg_ghcn_measurements.sql
    -------------------------
    Cleans raw GHCN measurements from BigQuery.

    Key transformations applied:
    1. Filter out quality-flagged rows (q_flag not blank = failed QA check)
    2. Filter only the 5 core weather elements (TMAX, TMIN, PRCP, SNOW, SNWD)
    3. Convert units: TMAX/TMIN from tenths of °C to °C (divide by 10)
    4. Convert units: PRCP from tenths of mm to mm (divide by 10)
    5. Filter out -9999 sentinel values (NOAA's missing data code)
    6. Parse date string into proper DATE type
    7. Extract year and month for easier downstream aggregation
*/

with raw as (
    select
        country_code,
        station_id,
        measurement_date,
        element,
        data_value,
        q_flag,
        s_flag
    from {{ source('ghcn_weather', 'ghcn_measurements') }}
    where
        -- Remove quality-flagged readings
        -- Blank q_flag means passed all QA checks
        (q_flag is null or trim(q_flag) = '')

        -- Focus on 5 core weather elements only
        and element in ('TMAX', 'TMIN', 'PRCP', 'SNOW', 'SNWD')

        -- Remove NOAA sentinel value for missing data
        and data_value != -9999
        and data_value is not null
),

converted as (
    select
        country_code,
        station_id,
        measurement_date,
        element,

        -- Convert units based on element type
        case
            when element in ('TMAX', 'TMIN')
                then round(data_value / 10.0, 1)  -- tenths of °C → °C
            when element = 'PRCP'
                then round(data_value / 10.0, 1)  -- tenths of mm → mm
            else data_value                         -- SNOW, SNWD already in mm
        end as measurement_value,

        -- Unit label for clarity
        case
            when element in ('TMAX', 'TMIN') then 'celsius'
            when element in ('PRCP', 'SNOW', 'SNWD') then 'mm'
        end as unit,

        -- Useful time dimensions for dashboard
        extract(year from measurement_date)  as measurement_year,
        extract(month from measurement_date) as measurement_month

    from raw
)

select * from converted
