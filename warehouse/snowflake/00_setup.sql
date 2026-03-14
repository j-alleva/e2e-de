-- ==========================================
-- 00_setup.sql
-- Purpose: Provision infrastructure and AWS handshake
-- ==========================================

CREATE WAREHOUSE IF NOT EXISTS de_wh 
    WITH WAREHOUSE_SIZE = 'XSMALL' 
    AUTO_SUSPEND = 60 
    AUTO_RESUME = TRUE 
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS warehouse;
USE DATABASE warehouse;

CREATE SCHEMA IF NOT EXISTS public;
USE SCHEMA public;

CREATE STORAGE INTEGRATION IF NOT EXISTS s3_init
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = 'S3'
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = '<YOUR_AWS_ROLE_ARN>'
    STORAGE_ALLOWED_LOCATIONS = ('<YOUR_S3_BUCKET_PATH>');

-- ==============================================================================
-- 0. Staging Layer (Raw Data from S3)
-- ==============================================================================
CREATE OR REPLACE TRANSIENT TABLE raw_weather_staging (
    time TIMESTAMP,
    temp_celsius FLOAT,
    humidity_percent INT,
    wind_speed_kmh FLOAT,
    precipitation_mm FLOAT,
    temp_fahrenheit FLOAT,
    is_freezing BOOLEAN,
    latitude FLOAT,
    longitude FLOAT,
    location VARCHAR(100),
    run_date DATE
);
-- ==============================================================================
-- 1. Dimension Tables
-- ==============================================================================
CREATE OR REPLACE TABLE dim_date (
    date_id INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    date_value DATE UNIQUE NOT NULL,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

CREATE OR REPLACE TABLE dim_location (
    location_id INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    location_name VARCHAR(100) UNIQUE NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);
-- ==============================================================================
-- 2. Fact Tables
-- ==============================================================================
CREATE OR REPLACE TABLE fact_weather_hourly (
    fact_id INT AUTOINCREMENT START 1 INCREMENT 1 PRIMARY KEY,
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    location_id INT NOT NULL REFERENCES dim_location(location_id),
    hour INT NOT NULL,
    temp_celsius FLOAT,
    humidity_percent INT,
    precipitation_mm FLOAT,
    wind_speed_kmh FLOAT,
    extraction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);