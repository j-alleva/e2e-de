-- ==============================================================================
-- 03_copy_into.sql
-- Purpose: Idempotent ELT load from S3 Gold into Star Schema
-- ==============================================================================
USE DATABASE warehouse;
USE SCHEMA public;

-- ------------------------------------------------------------------------------
-- Step 1: Extract & Load (Staging)
-- ------------------------------------------------------------------------------
TRUNCATE TABLE raw_weather_staging;

COPY INTO raw_weather_staging (
    time, temp_celsius, humidity_percent, wind_speed_kmh, precipitation_mm,
    temp_fahrenheit, is_freezing, latitude, longitude, location, run_date
)
FROM (
    SELECT
        $1:time::TIMESTAMP,
        $1:temp_celsius::FLOAT,
        $1:humidity_percent::INT,
        $1:wind_speed_kmh::FLOAT,
        $1:precipitation_mm::FLOAT,
        $1:temp_fahrenheit::FLOAT,
        $1:is_freezing::BOOLEAN,
        $1:latitude::FLOAT,
        $1:longitude::FLOAT,
        -- Extract location from the S3 folder path (e.g., location=Boston)
        SPLIT_PART(REGEXP_SUBSTR(METADATA$FILENAME, 'location=[^/]+'), '=', 2)::VARCHAR AS location,
        -- Extract run_date from the S3 folder path (e.g., run_date=2026-01-25)
        SPLIT_PART(REGEXP_SUBSTR(METADATA$FILENAME, 'run_date=[^/]+'), '=', 2)::DATE AS run_date
    FROM @my_s3_gold_stage
);

-- ------------------------------------------------------------------------------
-- Step 2: Transform & Upsert Dimensions
-- ------------------------------------------------------------------------------

-- 2a. Upsert Location Dimension
MERGE INTO dim_location AS target
USING (
    SELECT DISTINCT location, latitude, longitude
    FROM raw_weather_staging
) AS source
ON target.location_name = source.location
WHEN NOT MATCHED THEN
    INSERT (location_name, latitude, longitude)
    VALUES (source.location, source.latitude, source.longitude);

-- 2b. Upsert Date Dimension
MERGE INTO dim_date AS target
USING (
    SELECT DISTINCT
        TO_DATE(time) AS date_value,
        YEAR(time) AS year,
        MONTH(time) AS month,
        DAY(time) AS day,
        DAYOFWEEK(time) AS day_of_week,
        CASE WHEN DAYOFWEEK(time) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM raw_weather_staging
) AS source
ON target.date_value = source.date_value
WHEN NOT MATCHED THEN
    INSERT (date_value, year, month, day, day_of_week, is_weekend)
    VALUES (source.date_value, source.year, source.month, source.day, source.day_of_week, source.is_weekend);

-- ------------------------------------------------------------------------------
-- Step 3: Transform & Upsert Facts (The Idempotent Core)
-- ------------------------------------------------------------------------------
-- Match on the natural key: date_id + location_id + hour.
MERGE INTO fact_weather_hourly AS target
USING (
    SELECT 
        d.date_id,
        l.location_id,
        HOUR(r.time) AS hour,
        r.temp_celsius,
        r.humidity_percent,
        r.precipitation_mm,
        r.wind_speed_kmh
    FROM raw_weather_staging r
    JOIN dim_date d ON d.date_value = TO_DATE(r.time)
    JOIN dim_location l ON l.location_name = r.location
) AS source
ON target.date_id = source.date_id 
   AND target.location_id = source.location_id 
   AND target.hour = source.hour
WHEN MATCHED THEN
    -- Update on same run date / location instead of duplicating
    UPDATE SET 
        target.temp_celsius = source.temp_celsius,
        target.humidity_percent = source.humidity_percent,
        target.precipitation_mm = source.precipitation_mm,
        target.wind_speed_kmh = source.wind_speed_kmh,
        target.extraction_time = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    -- If new run date / location / hour, insert a new row
    INSERT (date_id, location_id, hour, temp_celsius, humidity_percent, precipitation_mm, wind_speed_kmh)
    VALUES (source.date_id, source.location_id, source.hour, source.temp_celsius, source.humidity_percent, source.precipitation_mm, source.wind_speed_kmh);