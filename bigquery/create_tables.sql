-- ============================================================
-- GHCN Weather Analytics — BigQuery Table Setup
-- ============================================================
-- Step 1: External table (reads directly from GCS Parquet)
-- Step 2: Native partitioned + clustered table (filtered)
-- ============================================================

-- Step 1: External table over raw GCS Parquet files
-- This reads all 187M rows directly from GCS without copying data
CREATE OR REPLACE EXTERNAL TABLE `mythic-altar-485103-v8.ghcn_weather.ext_ghcn_measurements`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ghcn-weather-datalake-mythic-altar-485103-v8/raw/yearly/*.parquet']
);

-- External table for stations metadata
CREATE OR REPLACE EXTERNAL TABLE `mythic-altar-485103-v8.ghcn_weather.ext_ghcn_stations`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://ghcn-weather-datalake-mythic-altar-485103-v8/raw/metadata/*.parquet']
);

-- ============================================================
-- Step 2: Native partitioned + clustered table
-- ============================================================
-- PARTITIONING by measurement_date (DATE type):
--   Each partition holds one day of data across all stations.
--   Our dashboard queries filter by date ranges (e.g. "show 2022 data").
--   Partitioning means BigQuery scans only relevant date partitions
--   instead of all 187M rows — dramatically reducing query cost and time.
--
-- CLUSTERING by country_code, element:
--   Within each date partition, rows are physically sorted by country_code
--   then element. Our dashboard queries always filter by country and element
--   (e.g. "show TMAX for IN, US, GB"). Clustering makes these filters
--   fast because BigQuery skips irrelevant blocks automatically.
-- ============================================================
CREATE OR REPLACE TABLE `mythic-altar-485103-v8.ghcn_weather.ghcn_measurements`
PARTITION BY measurement_date
CLUSTER BY country_code, element
AS
SELECT
  -- Extract country code from first 2 chars of station_id
  -- e.g. "AE000041196" -> "AE" (United Arab Emirates)
  LEFT(station_id, 2)                          AS country_code,
  station_id,
  -- Convert string date "20240101" -> proper DATE type
  PARSE_DATE('%Y%m%d', date)                   AS measurement_date,
  element,
  data_value,
  m_flag,
  q_flag,
  s_flag,
  obs_time
FROM
  `mythic-altar-485103-v8.ghcn_weather.ext_ghcn_measurements`
WHERE
  -- Filter to our 25 representative countries across all continents
  LEFT(station_id, 2) IN (
    -- North America
    'US', 'CA', 'MX',
    -- South America
    'BR', 'AR', 'CL',
    -- Europe
    'UK', 'GM', 'FR', 'IT', 'SP', 'SW', 'NO',
    -- Africa
    'SF', 'NI', 'EG', 'KE', 'MO',
    -- Asia
    'IN', 'CH', 'JA', 'SG', 'SA', 'TU',
    -- Oceania
    'AS', 'NZ'
  );