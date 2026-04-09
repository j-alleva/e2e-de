"""
End-to-End Weather Pipeline DAG
Orchestrates: Ingestion -> AWS Glue ETL -> Snowflake ELT -> dbt Analytics
Parameterized by {{ ds }} (logical run date) for idempotency and backfills.
"""

import os
from dotenv import dotenv_values
from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from docker.types import Mount

HOST_PROJECT_PATH = os.getenv("HOST_PROJECT_PATH")
if not HOST_PROJECT_PATH:
    raise ValueError("HOST_PROJECT_PATH not set in .env") 

env_vars = dotenv_values("/opt/airflow/.env")

default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
# --- Snowflake ELT ---
SNOWFLAKE_LOAD_SQL = """
USE DATABASE warehouse;
USE SCHEMA public;

TRUNCATE TABLE raw_weather_staging;

-- 1. Load current run_date partition from S3 Gold Stage
COPY INTO raw_weather_staging (
    time, temp_celsius, humidity_percent, wind_speed_kmh, precipitation_mm,
    temp_fahrenheit, is_freezing, latitude, longitude, location, run_date
)
FROM (
    SELECT
        $1:time::TIMESTAMP, $1:temp_celsius::FLOAT, $1:humidity_percent::INT,
        $1:wind_speed_kmh::FLOAT, $1:precipitation_mm::FLOAT, $1:temp_fahrenheit::FLOAT,
        $1:is_freezing::BOOLEAN, $1:latitude::FLOAT, $1:longitude::FLOAT,
        SPLIT_PART(REGEXP_SUBSTR(METADATA$FILENAME, 'location=[^/]+'), '=', 2)::VARCHAR AS location,
        SPLIT_PART(REGEXP_SUBSTR(METADATA$FILENAME, 'run_date=[^/]+'), '=', 2)::DATE AS run_date
    FROM @my_s3_gold_stage/run_date={{ ds }}/
);

-- 2a. Upsert Location Dimension
MERGE INTO dim_location AS target
USING (SELECT DISTINCT location, latitude, longitude FROM raw_weather_staging) AS source
ON target.location_name = source.location
WHEN NOT MATCHED THEN
    INSERT (location_name, latitude, longitude)
    VALUES (source.location, source.latitude, source.longitude);

-- 2b. Upsert Date Dimension
MERGE INTO dim_date AS target
USING (
    SELECT DISTINCT
        TO_DATE(time) AS date_value, YEAR(time) AS year, MONTH(time) AS month,
        DAY(time) AS day, DAYOFWEEK(time) AS day_of_week,
        CASE WHEN DAYOFWEEK(time) IN (0, 6) THEN TRUE ELSE FALSE END AS is_weekend
    FROM raw_weather_staging
) AS source
ON target.date_value = source.date_value
WHEN NOT MATCHED THEN
    INSERT (date_value, year, month, day, day_of_week, is_weekend)
    VALUES (source.date_value, source.year, source.month, source.day, source.day_of_week, source.is_weekend);

-- 3. Upsert Facts (Natural Key Match)
MERGE INTO fact_weather_hourly AS target
USING (
    SELECT 
        d.date_id, l.location_id, HOUR(r.time) AS hour, r.temp_celsius,
        r.humidity_percent, r.precipitation_mm, r.wind_speed_kmh
    FROM raw_weather_staging r
    JOIN dim_date d ON d.date_value = TO_DATE(r.time)
    JOIN dim_location l ON l.location_name = r.location
) AS source
ON target.date_id = source.date_id 
   AND target.location_id = source.location_id 
   AND target.hour = source.hour
WHEN MATCHED THEN
    UPDATE SET 
        target.temp_celsius = source.temp_celsius, target.humidity_percent = source.humidity_percent,
        target.precipitation_mm = source.precipitation_mm, target.wind_speed_kmh = source.wind_speed_kmh,
        target.extraction_time = CURRENT_TIMESTAMP()
WHEN NOT MATCHED THEN
    INSERT (date_id, location_id, hour, temp_celsius, humidity_percent, precipitation_mm, wind_speed_kmh)
    VALUES (source.date_id, source.location_id, source.hour, source.temp_celsius, source.humidity_percent, source.precipitation_mm, source.wind_speed_kmh);
"""

# --- DAG Definition ---
with DAG(
    dag_id='weather_end_to_end_pipeline',
    default_args=default_args,
    description='Automated ELT Pipeline: Ingest -> Glue -> Snowflake -> dbt',
    schedule_interval='@daily',
    catchup=False, 
    max_active_runs=1,
    tags=['portfolio', 'production'],
) as dag:

    # Task 1: Python API Extraction
    extract_to_s3 = DockerOperator(
        task_id='extract_to_bronze_silver',
        image='de-ingest:latest',
        api_version='auto',
        auto_remove='force',
        command='--run-date {{ ds }} --location "Boston" --write-s3',
        docker_url='unix://var/run/docker.sock',
        network_mode='e2e-de_default',
        environment=env_vars,
        mounts=[Mount(source=f"{HOST_PROJECT_PATH}/data", target="/app/data", type="bind")]
    )

    # Task 2: AWS Glue PySpark Transformation
    transform_glue_gold = GlueJobOperator(
        task_id='transform_glue_gold',
        job_name='silver_to_gold_etl', 
        script_args={
            '--run_date': '{{ ds }}',
            '--silver_bucket_prefix': env_vars.get("SILVER_BUCKET_PATH"),
            '--gold_bucket_prefix': env_vars.get("GOLD_BUCKET_PATH")
        },
        aws_conn_id='aws_default',
        wait_for_completion=True,
    )

    # Task 3: Snowflake Staging & Upserts
    load_snowflake_warehouse = SnowflakeOperator(
        task_id='load_snowflake_warehouse',
        snowflake_conn_id='snowflake_default',
        sql=SNOWFLAKE_LOAD_SQL,
    )

    # Task 4: dbt Analytics & Metric Compilation
    run_dbt_analytics = DockerOperator(
        task_id='run_dbt_analytics',
        image='ghcr.io/dbt-labs/dbt-snowflake:latest',
        api_version='auto',
        auto_remove='force',
        command='build --profiles-dir . --project-dir .',
        docker_url='unix://var/run/docker.sock',
        environment=env_vars,
        mounts=[Mount(source=f"{HOST_PROJECT_PATH}/dbt/de_dbt", target="/usr/app", type="bind")],
        working_dir='/usr/app',
        mount_tmp_dir=False
    )

    # --- Pipeline Orchestration Order ---
    extract_to_s3 >> transform_glue_gold >> load_snowflake_warehouse >> run_dbt_analytics