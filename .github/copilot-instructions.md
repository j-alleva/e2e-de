# AI Copilot Instructions for e2e-de

## Project Overview
This is an **end-to-end data engineering pipeline** orchestrating data extraction → transformation → governance → visualization using Airflow, Spark/AWS Glue, dbt, and Snowflake with metric definitions.

## Architecture & Data Flow

### Layer Structure (Top to Bottom)
1. **Ingest Layer** (`src/pipeline/ingest/`): Fetch raw data from external sources
   - `fetch.py` - Retrieve data via APIs or databases
   - `normalize.py` - Standardize formats, column names, data types
   - `validate.py` - Schema validation, null checks, data quality rules
   
2. **Processing Layer** (`src/pipeline/transform/` + `spark/glue_job.py`):
   - `pandas_transform.py` - Python-based transformations for smaller datasets
   - `glue_job.py` - AWS Glue/Spark jobs for large-scale distributed processing
   
3. **Storage & Orchestration**:
   - **PostgreSQL** (`sql/postgres/`) - Staging layer for raw/normalized data
   - **Snowflake** - Analytics data warehouse (configured via dbt)
   - **Airflow DAGs** (`airflow/dags/`) - Workflow orchestration and scheduling

4. **Transformation Governance** (`dbt/de_dbt/`):
   - `models/staging/` - Raw to analytics (1:1 transformations)
   - `models/marts/` - Analytics-ready fact/dimension tables
   - `metrics.yml` - Governed metric definitions for consistency across tools

5. **Visualization** (`streamlit/app.py`) - User-facing dashboards

### Key Integration Points
- **Airflow → Spark**: Triggers `glue_job.py` for distributed processing
- **Spark → PostgreSQL/Snowflake**: Outputs transformed data to staging/warehouse
- **dbt → Snowflake**: Transforms staged data using SQL models with tests
- **Streamlit → Snowflake**: Queries analytics tables for dashboards

## Developer Workflows

### Local Development
- **Config Management**: `src/pipeline/config.py` - Environment variables, data paths, connection strings (use `.env` with `python-dotenv`)
- **Dependencies**: `requirements.txt` (pandas, requests, pyarrow, fastparquet, python-dotenv)
- **Docker Setup**: `docker/docker-compose.yml` provides local Postgres, Airflow, and services

### Testing & Validation
- **Unit Tests**: `tests/` directory - test ingest validation, transform logic, dbt model contracts
- **dbt Tests**: Leverage dbt's built-in tests (not_null, unique, relationships) in model YAML files
- **Data Quality**: `validate.py` should check for:
  - Schema compliance
  - Null/duplicate handling
  - Business logic constraints

### SQL Reference
- `sql/postgres/01_create_tables.sql` - Source/staging table schemas
- `sql/postgres/02_star_schema.sql` - Denormalized analytics schemas (if applicable)
- `sql/queries/` - Reusable SQL snippets and reports

## Project Conventions

### File Organization
- **Python modules**: Follow layered structure (`ingest` → `transform` → `io`)
- **dbt Naming**: 
  - Staging: `stg_{source}_{entity}.sql` (snake_case, single sources)
  - Marts: `{entity}_facts.sql`, `{entity}_dims.sql` (business-aligned)
- **Airflow**: One DAG file per logical workflow, idempotent task design

### I/O Abstraction
- **`src/pipeline/io/`**: Handles multiple storage backends
  - `local.py` - Local filesystem (development)
  - `s3.py` - AWS S3 (production)
- Use abstraction layer to avoid hardcoded paths in business logic

### Configuration Pattern
```python
# src/pipeline/config.py should centralize:
DATA_SOURCE_URL = os.getenv('DATA_SOURCE_URL')
WAREHOUSE_CONN = os.getenv('SNOWFLAKE_CONN_STRING')
STAGING_PATH = os.getenv('STAGING_PATH', './data/staging')
```

## Common Commands & Workflows
```bash
# Docker local environment
docker-compose up  # Starts Airflow, Postgres, supporting services

# dbt commands (from dbt/de_dbt/)
dbt run          # Execute models in dependency order
dbt test         # Run model tests and source freshness checks
dbt docs generate # Build documentation
dbt seed         # Load CSV data into warehouse

# Run specific ingest pipeline
python -m src.pipeline.ingest.fetch
python -m src.pipeline.ingest.normalize
python -m src.pipeline.ingest.validate

# AWS Glue (requires AWS CLI/SDK)
aws glue start-job-run --job-name glue_job --region us-east-1
```

## Critical Patterns to Preserve

1. **Idempotency**: All pipeline steps must be safely re-runnable (no duplicate inserts)
2. **Lineage Tracking**: Use dbt source freshness and Airflow task dependencies
3. **Staged Ingestion**: Never load raw data directly to analytics warehouse - use PostgreSQL staging first
4. **Schema Evolution**: Test backward compatibility when modifying table schemas
5. **Metric Governance**: Centralize metric definitions in `metrics.yml` to prevent ad-hoc dashboard calculations

## When Making Changes
- **Adding data sources**: Implement `fetch.py` consumer, add validation rules to `validate.py`
- **New transforms**: Add as dbt model in staging or marts with tests
- **Schema changes**: Update both dbt model and SQL reference files; consider downstream impacts
- **Scaling processing**: Migrate pandas logic to `glue_job.py` when handling >100MB datasets
- **New visualizations**: Add Streamlit pages, ensure data queries pull from dbt marts (not staging)

## Project Timeline

Week 1 — Python ingestion + cleaning (local)
Build

A Python CLI/script like:

python -m pipeline.run --run-date 2026-01-25 --location "Boston"

Deliverables (Definition of Done)

Pull from a public API (Open-Meteo is fine) and write:

data/bronze/source=openmeteo/run_date=YYYY-MM-DD/raw.json

data/silver/source=openmeteo/run_date=YYYY-MM-DD/clean.parquet

(optional) also clean.csv for quick inspection

Basic validation:

required fields exist

timestamp parseable

no duplicates on your natural key (example: location + timestamp)

Logging includes:

number of records fetched

number of records after cleaning

output paths written

Resume evidence you’re building

“Built parameterized ingestion job with schema validation and reproducible run-date partitioning; wrote raw JSON and cleaned Parquet outputs with logging.”

Week 2 — SQL foundations + modeling (Postgres in Docker)
Build

docker compose up -d postgres

Load Week 1 cleaned Parquet/CSV into Postgres (simple loader script).

Create a simple analytics-friendly schema:

dim_date (or time dimension)

dim_location

fact_weather_hourly (or your equivalent fact)

Deliverables

docker-compose.yml with Postgres

sql/postgres/ contains:

create tables

insert/load

12–15 queries saved as .sql files demonstrating:

GROUP BY

JOIN

window functions: RANK, LAG/LEAD, rolling averages, partitioned aggregates

README section: “Data model + sample analytical queries”

Key reason this matters

This week converts “I can do SQL interview questions” into “I can model and query a dataset like a data engineer.”

Week 3 — AWS fundamentals: IAM + S3 + lake layout
Build

AWS account + least privilege IAM user/role for programmatic S3 access

S3 bucket + folder conventions (bronze/silver/gold)

Modify Week 1 ingestion to:

write locally AND upload to S3 (bronze + silver)

keys include run_date=YYYY-MM-DD

Deliverables

A small s3.py client wrapper (boto3)

A --write-s3 flag (or config) that uploads outputs

A short infra.md explaining:

bucket name conventions

prefix layout

how IAM permissions are scoped

Resume evidence

“Implemented S3-based lake layout (bronze/silver/gold) with least-privilege IAM and automated partitioned uploads by run date.”

Week 4 — Dockerization + CI + publish early
Build

Dockerize your ingestion job:

docker build -t de-ingest .

docker run ... de-ingest --run-date ...

GitHub Actions:

lint (ruff/flake8)

unit tests (pytest)

(optional) type check (mypy)

Deliverables

Public GitHub repo with:

README that includes:

what the pipeline does

architecture diagram placeholder (you’ll add the real one in Week 11)

quickstart commands

CI passing badge (optional but strong)

Recruiting action (start here)

From this point onward, you have enough to apply broadly because you have:

a real repo

a real pipeline

cloud storage integration

Docker + CI hygiene

Week 5 — Orchestration (Airflow in Docker)
Build

Airflow docker-compose (local)

DAG with tasks:

run_ingestion_container (DockerOperator) with run_date parameter

Add retries, logging, and backfill capability.

Deliverables

DAG file in airflow/dags/

README section: “Orchestration + how to run Airflow locally”

Demonstrate at least:

one successful scheduled run

one manual backfill run with a past run_date

Resume evidence

“Orchestrated scheduled ingestion using Airflow with parameterized run dates, retries, and backfills.”

Week 6 — Spark transformations on AWS Glue (the real Spark credibility week)
Build

AWS Glue Spark job:

Input: S3 bronze or silver

Output: S3 gold as Parquet

Partition by run_date (and optionally location)

Your “gold” should be curated: consistent types, renamed columns, derived metrics, etc.

Deliverables

spark/glue_job.py (or Glue script)

Evidence of at least 2 partitions written:

run_date=... for two different dates

A validation step (even simple):

record counts between silver and gold match expected logic

Resume evidence

“Built Spark ETL in AWS Glue to generate partitioned Parquet curated datasets from S3 raw zones.”

Week 7 — Snowflake warehouse load (you requested Snowflake explicitly)
Build

Snowflake trial setup:

database + schema

warehouse

Load curated S3 gold Parquet into Snowflake.

Minimum viable: create stage + file format + COPY INTO

Stronger: staged table → merge into final fact table for idempotency

Deliverables

warehouse/snowflake/ folder with:

00_setup.sql (db/schema/warehouse)

01_file_formats.sql

02_stages.sql

03_copy_into.sql

04_validation.sql (row counts, null checks)

At least 1 curated table in Snowflake (fact_*) populated and queryable

Resume evidence

“Loaded S3-curated Parquet into Snowflake using staged ingestion patterns and validation queries.”

Week 8 — dbt transformations + tests + docs (on Snowflake)
Build

dbt project with:

staging models (light cleaning/renaming)

marts model (a business-friendly aggregate)

tests:

not_null, unique, relationships

docs generation (dbt docs generate)

Add CI step: dbt compile (or dbt build later once stable)

Deliverables

dbt/ folder is runnable end-to-end

Screenshot of dbt DAG + docs page (portfolio proof)

Resume evidence

“Built modular analytics layer in dbt on Snowflake with automated tests and generated documentation.”

Week 9 — Semantic metrics + end-to-end Airflow DAG
Build

Define metrics in dbt (semantic layer):

e.g., daily avg temperature, rolling 7-day average, precipitation totals, etc.

Update Airflow DAG to run the full chain:

ingestion container → S3 bronze/silver

Glue job → S3 gold

Snowflake load → raw/staging tables

dbt run/test → marts + metrics

Deliverables

One DAG that can run for any run_date (manual trigger/backfill)

Clear idempotency behavior documented:

what happens if you rerun the same date?

how does Snowflake load avoid duplicates?

Resume evidence

“Architected end-to-end ELT orchestration (Airflow → Spark/Glue → Snowflake → dbt) with governed metrics definitions.”

Week 10 — Streamlit app (consumption layer)
Build

Streamlit app connects to Snowflake and renders:

KPI cards (2–4)

2–3 charts

Strong recommendation: only query dbt marts/metrics, not raw tables.

Add caching to reduce repeated queries.

Deliverables

streamlit/app.py runnable locally

README includes screenshots + “how to run the dashboard”

Resume evidence

“Delivered stakeholder-facing analytics app in Streamlit powered by Snowflake marts and governed dbt metrics.”

Week 11 — Production hardening + portfolio polish
Build artifacts recruiters actually care about:

Architecture diagram (Excalidraw)

Runbook:

how to run locally

common failures + fixes

how to backfill

Cost notes (high-level)

Failure modes:

API down

schema drift

partial runs

Data contracts / checks:

schema expectations

row count anomalies

freshness checks (even manual)

Deliverables

docs/architecture.png

docs/runbook.md

docs/cost_and_limits.md

A short demo GIF or screenshot sequence

Week 12 — Fall 2026 application blitz + interview readiness
Outputs

Final resume bullets (project section)

3–5 STAR stories:

debugging IAM

making loads idempotent

designing bronze/silver/gold layout

Spark vs Pandas tradeoff

Airflow retries/backfills design

A 1-page case study:

problem → architecture → decisions → outcomes → next steps

Weekly application cadence plan (repeatable)
---
*Last Updated: 2026-01-29*
