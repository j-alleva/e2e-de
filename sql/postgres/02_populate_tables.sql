-- 02_populate_tables.sql
-- Purpose: Transforms raw staging data into the Star Schema (Dimensions & Facts).
-- This script is idempotent: it clears target tables before reloading them
-- to prevent duplicate data during development.

-- ==============================================================================
-- 1. Clean Slate
-- ==============================================================================

-- Clear out the Fact table first (because it depends on Dimensions)
TRUNCATE TABLE fact_weather_hourly CASCADE;

-- Clear out Dimensions
TRUNCATE TABLE dim_location CASCADE;
TRUNCATE TABLE dim_date CASCADE;


-- ==============================================================================
-- 2. Populate Dimensions
-- ==============================================================================

-- 2a. Populate Location Dimension
-- Logic: Select distinct locations from raw_weather and insert them.
INSERT INTO dim_location (location_name, latitude, longitude)
SELECT DISTINCT 
    location,
    latitude,
    longitude
FROM raw_weather
WHERE location IS NOT NULL;

-- 2b. Populate Date Dimension
-- Logic: Extract unique dates and calculate attributes (Year, Month, Weekend).
INSERT INTO dim_date (date_value, year, month, day, day_of_week, is_weekend)
SELECT DISTINCT 
    CAST(time AS DATE) AS date_value,
    EXTRACT(YEAR FROM time) AS year,
    EXTRACT(MONTH FROM time) AS month,
    EXTRACT(DAY FROM time) AS day,
    EXTRACT(DOW FROM time) AS day_of_week,
    CASE WHEN EXTRACT(DOW FROM time) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
FROM raw_weather;


-- ==============================================================================
-- 3. Populate Fact Table
-- ==============================================================================

-- 3a. Populate Hourly Weather Fact
-- Logic: Join raw_weather with dimensions to replace text with IDs.
INSERT INTO fact_weather_hourly (
    date_id, 
    location_id, 
    hour,
    temperature_2m, 
    relative_humidity_2m, 
    precipitation, 
    wind_speed_10m,
    extraction_time
)
SELECT 
    d.date_id,
    l.location_id,
    EXTRACT(HOUR FROM r.time) as hour,
    r.temperature_2m,
    r.relative_humidity_2m,
    r.precipitation,
    r.wind_speed_10m,
    NOW() as extraction_time
FROM raw_weather r
-- Join to Date Dimension to get date_id
JOIN dim_date d ON d.date_value = DATE(r.time)
-- Join to Location Dimension to get location_id
JOIN dim_location l ON l.location_name = r.location;


