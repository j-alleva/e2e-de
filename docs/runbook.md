
# Pipeline Runbook & Operations Guide

This runbook outlines the operational procedures for executing, monitoring, and troubleshooting the end-to-end weather data pipeline locally.

## 1. Local Environment Setup & Execution

The project relies on a Makefile to abstract away Docker network configurations and provide a streamlined developer workflow.


### Prerequisites

Before running the pipeline, ensure your `.env` file is populated. You must explicitly set `HOST_PROJECT_PATH` (your absolute local directory path) as it is required for Airflow's DockerOperator volume mounts.

The documented commands assume a Bash-compatible shell. On Windows, prefer Git Bash or WSL so the Makefile targets and multiline commands run as written.

### Quick Start

To spin up the entire infrastructure from scratch:

**Build the Ingestion Image:**
```bash
make build
```

**Initialize the Airflow Database:**
```bash
make airflow-init
```

**Start the Orchestrator and Warehouse:**
```bash
make airflow-up
```

**Access the UIs:**
- Airflow UI: `localhost:8080` (Credentials: `airflow` / `airflow`)
- Postgres Warehouse (Local): `localhost:5432` (Credentials: `admin` / `password`)

**Run the Analytics Dashboard:**
```bash
make app
```
Streamlit will be available at http://localhost:8501

### Teardown

To safely spin down the infrastructure and preserve volume data:
```bash
make down
```

To completely wipe local storage (Bronze/Silver layers):
```bash
make clean
```

## 2. Orchestration & Backfilling

The primary DAG (`weather_end_to_end_pipeline`) is heavily parameterized using Airflow's logical execution date (`{{ ds }}`). This guarantees idempotency across S3 partitions and Snowflake MERGE statements.

### Standard Execution

The DAG is scheduled to run `@daily`. You can manually trigger a run for the current date by navigating to the Airflow UI, unpausing the DAG, and clicking the Play button.

### Executing a Historical Backfill

Because the pipeline uses `{{ ds }}`, you can safely backfill historical data without duplicating records in Snowflake or overriding the wrong S3 partitions.

To run a backfill across a specific date range, execute the following from your host terminal:
```bash
docker exec -it airflow_scheduler airflow dags backfill \
    -s 2026-01-01 -e 2026-01-31 weather_end_to_end_pipeline
```

## 3. Common Failure Modes & Resolutions

### 1. DockerOperator Fails to Mount Volumes

**Symptoms:** The `extract_to_bronze_silver` or `run_dbt_analytics` tasks fail immediately with a "file not found" or "invalid mount" error in the Airflow logs.

**Root Cause:** The `HOST_PROJECT_PATH` environment variable is either missing in the `.env` file or points to an incorrect directory, preventing the Docker socket from binding the local `/data` or `/dbt` folders to the temporary containers.

**Resolution:** Verify `HOST_PROJECT_PATH` in `.env` matches your absolute project path. In a Bash-compatible shell, this should match your exact `$(pwd)` output. Restart Airflow (`make down` then `make airflow-up`).

### 2. GlueJobOperator Fails or Times Out

**Symptoms:** The `transform_glue_gold` task turns red after several minutes.

**Root Cause:** Usually an IAM permission issue, a missing S3 bucket, or a syntax error in the PySpark script.

**Resolution:** 
1. Check the AWS CloudWatch logs linked directly in the Airflow task instance details.
2. Verify the Airflow connection `aws_default` has valid credentials.
3. Ensure `SILVER_BUCKET_PATH` and `GOLD_BUCKET_PATH` in your `.env` correctly point to your exact bucket path.

### 3. Port Conflicts on Startup

**Symptoms:** `make airflow-up` throws an error: `bind: address already in use` for ports 5432 or 8080.

**Root Cause:** A local instance of PostgreSQL or another web application is already bound to those ports.

**Resolution:** Kill the conflicting local services, or run `make down` to ensure no orphaned Docker containers are holding the ports hostage.
