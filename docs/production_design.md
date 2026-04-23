
# Current Operational Design & Failure Modes

## 1. Known Failure Modes & Resiliency

### 1.1 Source API Downtime (Open-Meteo)

**Risk:** The external Open-Meteo API is unreachable, times out, or returns a 5xx server error.

**Mitigation:**
- The Airflow DockerOperator task (`extract_to_bronze_silver`) is configured with standard retry logic (e.g., 2 retries with a 5-minute delay).
- Because the pipeline is parameterized by logical run date (`{{ ds }}`), missed dates can be recovered by unpausing the DAG or triggering a historical backfill once the API is restored.

### 1.2 Upstream Schema Drift

**Risk:** Open-Meteo alters their JSON response payload (e.g., dropping a field, changing a data type, or renaming a key).

**Mitigation:**
- **Bronze Retention:** In the orchestrated path, the raw JSON payload is written to the Bronze path and uploaded to S3 before downstream transformations run. If normalization or later steps fail, the raw payload for that run date is still available for inspection and replay.
- **Fail-Fast Validation:** The Python validation logic checks for required top-level keys, parseable timestamps, and duplicate natural keys before normalization. If those checks fail, the task stops rather than passing malformed data downstream.

### 1.3 Partial Runs & Mid-Pipeline Failures

**Risk:** A task fails midway through execution (e.g., the Snowflake load succeeds, but the dbt analytics run fails), leaving the system in an intermediate state.

**Mitigation:**
- **Task Sequencing:** Airflow executes the pipeline in a fixed order: extraction, Glue transformation, Snowflake load, then dbt analytics. A downstream task does not run unless the upstream task succeeds.
- **Idempotent Reloads:** The Snowflake load uses `MERGE` statements on natural keys, so re-running the same `run_date` updates existing business keys instead of duplicating warehouse records.
- **Recoverability by Rerun:** If a later task fails, the pipeline can be rerun for the same logical date to rebuild downstream state from the staged Bronze, Silver, and Gold data already written for that partition.

## 2. Data Quality Controls

To maintain trust in the analytics layer, the pipeline enforces data quality contracts primarily through dbt and database constraints.

### 2.1 Schema Validation & Enforcement

- **Python Validation:** Bronze validation checks for required top-level JSON keys before normalization begins.
- **Glue Transformation:** The Silver-to-Gold Spark job renames core weather fields, converts `time` to timestamp, and derives analytics-ready columns such as Fahrenheit temperature and a freezing flag before writing Gold Parquet.
- **Warehouse Constraints:** Snowflake tables define primary keys, `NOT NULL` fields, `UNIQUE` constraints on key dimensions, and foreign-key references between facts and dimensions.
- **dbt Tests:** The dbt build step is part of the orchestration DAG, and the current mart model tests enforce `unique`, `not_null`, and `relationships` checks on `summary_id`, `date_id`, and `location_id`.

### 2.2 Row Count Anomalies

- **Glue Row Count Check:** The Silver-to-Gold Spark job compares the input Silver row count to the transformed Gold row count and fails the job if they differ.
- **dbt Referential Checks:** dbt `relationships` tests verify that mart foreign keys resolve to the expected staging dimensions. These checks validate referential integrity, even though they are not a full row-count reconciliation across every layer.

### 2.3 Freshness Monitoring

- **Scheduled Execution:** The DAG is scheduled to run daily through Airflow. If a scheduled run fails, the dashboard continues to reflect the most recently loaded warehouse state until the failed date is rerun successfully.
- **Operational Visibility:** Airflow task history provides the primary operational view of whether the daily pipeline succeeded or failed.
- **Dashboard Verification:** The Streamlit dashboard exposes the available date range from the analytics mart, which provides a simple manual check of how recent the loaded warehouse data is.
