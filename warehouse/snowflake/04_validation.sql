-- ==============================================================================
-- 04_validation.sql
-- Purpose: Data quality checks and portfolio proof
-- ==========================================
USE DATABASE warehouse;
USE SCHEMA public;

-- ------------------------------------------------------------------------------
-- 1. Row Count Validation
-- ------------------------------------------------------------------------------
SELECT 
    'fact_weather_hourly' AS table_name, 
    COUNT(*) AS row_count 
FROM fact_weather_hourly;

-- ------------------------------------------------------------------------------
-- 2. Null Checks on Critical Columns
-- ------------------------------------------------------------------------------
SELECT 
    COUNT(*) AS total_rows,
    SUM(CASE WHEN temp_celsius IS NULL THEN 1 ELSE 0 END) AS missing_temps,
    SUM(CASE WHEN precipitation_mm IS NULL THEN 1 ELSE 0 END) AS missing_precip,
    SUM(CASE WHEN location_id IS NULL OR date_id IS NULL THEN 1 ELSE 0 END) AS orphaned_records
FROM fact_weather_hourly;

-- ------------------------------------------------------------------------------
-- 3. The "Queryable" Proof (Analytical Join)
-- ------------------------------------------------------------------------------
SELECT 
    d.date_value,
    l.location_name,
    f.hour,
    f.temp_celsius,
    f.wind_speed_kmh
FROM fact_weather_hourly f
JOIN dim_location l ON f.location_id = l.location_id
JOIN dim_date d ON f.date_id = d.date_id
ORDER BY d.date_value DESC, l.location_name, f.hour
LIMIT 15;