-- ==========================================
-- 01_file_formats.sql
-- Purpose: Define Parquet parsing rules
-- ==========================================
USE DATABASE warehouse;
USE SCHEMA public;

CREATE OR REPLACE FILE FORMAT my_parquet_format
    TYPE = PARQUET
    COMPRESSION = AUTO;