# e2e-de
End-to-end data engineering platform demonstrating production-grade ingestion, transformation, orchestration, and analytics.

**Status:** Block 1 Complete (Python Ingestion + Local Storage)

--- 

## Block 1: Python Ingestion Pipeline

### Architecture Overview
```
API (Open-Meteo) → Python CLI (fetch → validate → normalize)
→ Local Data Lake (Bronze → Silver) → Parquet Files (analysis-ready)
```
### What's Implemented

- Parameterized CLI - Run pipeline for any date/location (Currently: Only default location supported and valid open-meteo dates)
- Bronze Layer - Raw JSON from Open-Meteo API (immutable)  
- Silver Layer - Cleaned Parquet with schema validation  
- Data Quality - Schema checks + duplicate detection on natural key  
- Idempotent - Safe to re-run same date without side effects  
- Logging - Console + file output with record counts  
- Production Docs - Google-style docstrings throughout  

### Quick Start

**Prerequisites:**

- Python 3.9+
- `pip install -r requirements.txt`

**Setup:**

1. Clone and install dependencies
```bash
git clone https://github.com/j-alleva/e2e-de.git
cd e2e-de
pip install -r requirements.txt 
```
2. Configure environment (optional)
```bash
cp .env.example .env
# (If you want custom paths)
```

3. Run the pipeline: 
```bash
python -m src.pipeline.run --run-date 2026-01-31 --location Boston
```
**Expected Output:**

Bronze Layer (raw JSON):
```
data/bronze/source=openmeteo/run_date=2026-01-31/location=Boston/raw.json
```
Silver Layer(cleaned Parquet):
```
data/silver/source=openmeteo/run_date=2026-01-31/location=Boston/weather_data.parquet
```
Logs:
```
pipeline.log (detailed execution log)
```

### Usage Examples:

Run for different dates:
```bash
python -m src.pipeline.run --run-date 2026-01-25 --location Boston
python -m src.pipeline.run --run-date 2026-01-26 --location Boston
```

Run for different locations (currently not supported, but can be added):
```bash
python -m src.pipeline.run --run-date 2026-01-31 --location "New York"
python -m src.pipeline.run --run-date 2026-01-31 --location Chicago
```

Get help:
```bash
python -m src.pipeline.run --help
```

### Data Lake Structure:

```
data/
├── bronze/           # Raw, immutable source data
│   └── source=openmeteo/
│       └── run_date=YYYY-MM-DD/
│           └── location=Boston/
│               └── raw.json
│
└── silver/           # Cleaned, validated, analysis-ready
    └── source=openmeteo/
        └── run_date=YYYY-MM-DD/
            └── location=Boston/
                └── weather_data.parquet
```

### Environment Variables:

See `.env.example` for config template

**Required:**

- `LOCAL_BRONZE_PATH` - Bronze layer storage path (default: ./data/bronze)
- `LOCAL_SILVER_PATH` - Silver layer storage path (default: ./data/silver)
- `OPEN_METEO_URL_TEMPLATE` - API endpoint with parameter placeholders

**Future (Blocks 3+):**

- AWS credentials (S3)
- Snowflake connection details

### Development:

Project Structure:
```
src/pipeline/
├── config.py              # Environment config + path generation
├── run.py                 # CLI entry point
└── ingest/
    ├── fetch.py           # API extraction → bronze
    ├── validate.py        # Data quality checks
    └── normalize.py       # Bronze → silver transformation
```

**Key Design Patterns:**

- **Idempotency:** Re-running same run_date overwrites outputs (no duplicates)
- **Partitioning:** All outputs partitioned by run_date for backfills
- **Validation:** Schema + data quality checks before transformation
- **Natural Key:** (location, timestamp) used for duplicate detection

### Testing:

Manual end-to-end test:

```bash
# Run pipeline
python -m src.pipeline.run --run-date 2026-01-31 --location Boston

# Verify bronze output
cat data/bronze/source=openmeteo/run_date=2026-01-31/location=Boston/raw.json

# Verify silver output exists
ls data/silver/source=openmeteo/run_date=2026-01-31/location=Boston/weather_data.parquet

# Check logs
tail -50 pipeline.log
```

### Troubleshooting:

**Problem:** `ModuleNotFoundError: No module named 'src'`
**Solution:** Run from project root: `python -m src.pipeline.run ...`

**Problem:** `FileNotFoundError` when running pipeline
**Solution:** Ensure data directories exist; pipeline creates them automatically

**Problem:** API request fails
**Solution:** Check internet connection; Open-Meteo is public and requires no API key


## Block 2: Postgres staging + SQL transformations
**To-do**:
- [ ] Load silver Parquet into Postgres staging tables
- [ ] Write SQL transformation (fact/dimension models)
- [ ] Add dbt-style tests in SQL
- [ ] Create 12 - 15 analytical queries

**Future Blocks:**
- Block 3: AWS S3 data lake
- Block 4: Docker + CI/CD
- Block 5: Airflow orchestration
- Block 6: Spark/Glue distributed processing
- Block 7: Snowflake warehouse
- Block 8: dbt transformations + tests
- Block 9: Semantic metrics layer
- Block 10: Streamlit dashboard

## Status
**Block 1:** Complete
**Last Updated:** January 31, 2026

**License:** MIT

