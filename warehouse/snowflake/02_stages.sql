-- ==========================================
-- 02_stages.sql
-- Purpose: Create the external stage pointing to S3 Gold data
-- ==========================================
USE DATABASE warehouse;
USE SCHEMA public;

CREATE OR REPLACE STAGE my_s3_gold_stage
    URL = '<YOUR_S3_BUCKET_PATH>'
    STORAGE_INTEGRATION = s3_init
    FILE_FORMAT = my_parquet_format;