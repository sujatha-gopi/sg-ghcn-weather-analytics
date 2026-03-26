/*
    stg_ghcn_stations.sql
    ----------------------
    Cleans GHCN station metadata.

    Key transformations:
    1. Remove stations with invalid coordinates
    2. Remove stations with missing names
    3. Add continent lookup based on country code
*/

with raw as (
    select
        station_id,
        country_code,
        latitude,
        longitude,
        elevation_m,
        station_name
    from {{ source('ghcn_weather', 'ext_ghcn_stations') }}
    where
        -- Remove stations with invalid coordinates
        latitude between -90 and 90
        and longitude between -180 and 180
        -- Remove stations with missing names
        and station_name is not null
        and trim(station_name) != ''
        -- Keep only our 25 selected countries
        and country_code in (
            'US','CA','MX','BR','AR','CL',
            'UK','GM','FR','IT','SP','SW','NO',
            'SF','NI','EG','KE','MO',
            'IN','CH','JA','SG','SA','TU',
            'AS','NZ'
        )
),

with_continent as (
    select
        station_id,
        country_code,

        -- Map FIPS country codes to readable names
        case country_code
            when 'US' then 'United States'
            when 'CA' then 'Canada'
            when 'MX' then 'Mexico'
            when 'BR' then 'Brazil'
            when 'AR' then 'Argentina'
            when 'CL' then 'Chile'
            when 'UK' then 'United Kingdom'
            when 'GM' then 'Germany'
            when 'FR' then 'France'
            when 'IT' then 'Italy'
            when 'SP' then 'Spain'
            when 'SW' then 'Sweden'
            when 'NO' then 'Norway'
            when 'SF' then 'South Africa'
            when 'NI' then 'Nigeria'
            when 'EG' then 'Egypt'
            when 'KE' then 'Kenya'
            when 'MO' then 'Morocco'
            when 'IN' then 'India'
            when 'CH' then 'China'
            when 'JA' then 'Japan'
            when 'SG' then 'Singapore'
            when 'SA' then 'Saudi Arabia'
            when 'TU' then 'Turkey'
            when 'AS' then 'Australia'
            when 'NZ' then 'New Zealand'
            else 'Unknown'
        end as country_name,

        -- Continent grouping for dashboard filters
        case country_code
            when 'US' then 'North America'
            when 'CA' then 'North America'
            when 'MX' then 'North America'
            when 'BR' then 'South America'
            when 'AR' then 'South America'
            when 'CL' then 'South America'
            when 'UK' then 'Europe'
            when 'GM' then 'Europe'
            when 'FR' then 'Europe'
            when 'IT' then 'Europe'
            when 'SP' then 'Europe'
            when 'SW' then 'Europe'
            when 'NO' then 'Europe'
            when 'SF' then 'Africa'
            when 'NI' then 'Africa'
            when 'EG' then 'Africa'
            when 'KE' then 'Africa'
            when 'MO' then 'Africa'
            when 'IN' then 'Asia'
            when 'CH' then 'Asia'
            when 'JA' then 'Asia'
            when 'SG' then 'Asia'
            when 'SA' then 'Asia'
            when 'TU' then 'Asia'
            when 'AS' then 'Oceania'
            when 'NZ' then 'Oceania'
            else 'Unknown'
        end as continent,

        latitude,
        longitude,
        elevation_m,
        station_name

    from raw
)

select * from with_continent
