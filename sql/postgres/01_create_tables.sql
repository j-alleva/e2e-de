-- 01_create_tables.sql
-- Purpose: Defines the schema for the data warehouse (star schema)
-- This script is idempotent: it drops existing tables before recreating them.

-- ==============================================================================
-- 0. Staging Layer (Raw Data from Ingestion)
-- ==============================================================================

-- 0a. Raw Weather Staging
-- Landing zone for silver Parquet data from load.py.
-- Gets replaced completely on each run (idempotent).

DROP TABLE IF EXISTS raw_weather;

CREATE TABLE raw_weather (
    time TIMESTAMP,
    temperature_2m FLOAT,
    relative_humidity_2m INT,
    precipitation FLOAT,
    wind_speed_10m FLOAT,
    latitude FLOAT,
    longitude FLOAT,
    location VARCHAR(100),
    source VARCHAR(50),
    run_date DATE
);

-- ==============================================================================
-- 1. Dimension Tables (The "Who, What, Where, When")
-- ==============================================================================

-- 1a. Date Dimension
-- Holds unique dates and derived attributes (year, month, day, day_of_week).
-- This allows us to slice data by "Weekends" or "Q1" easily later.
DROP TABLE IF EXISTS dim_date CASCADE;

CREATE TABLE dim_date (
    date_id SERIAL PRIMARY KEY,
    date_value DATE UNIQUE NOT NULL,
    year INT,
    month INT,
    day INT,
    day_of_week INT,
    is_weekend BOOLEAN
);

-- 1b. Location Dimension
-- Holds details about the places we are measuring.
DROP TABLE IF EXISTS dim_location CASCADE;

CREATE TABLE dim_location (
    location_id SERIAL PRIMARY KEY,
    location_name VARCHAR(100) UNIQUE NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL
);

-- ==============================================================================
-- 2. Fact Tables (The "Measurements")
-- ==============================================================================

-- 2a. Hourly Weather Fact
-- Holds the actual numerical measurements.
-- Links back to dimensions using Foreign Keys (FK).
DROP TABLE IF EXISTS fact_weather_hourly;

CREATE TABLE fact_weather_hourly (
    fact_id SERIAL PRIMARY KEY,
    date_id INT NOT NULL REFERENCES dim_date(date_id),
    location_id INT NOT NULL REFERENCES dim_location(location_id),
    hour INT NOT NULL,
    temperature_2m FLOAT,
    relative_humidity_2m INT,
    precipitation FLOAT,
    wind_speed_10m FLOAT,
    extraction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
