
# System Costs & Hard Limits

## 1. Compute Costs

Compute is the primary variable cost driver in this architecture. The cost controls documented here focus on low-cost warehouse sizing, automatic suspension, and serialized batch execution.

### Snowflake Warehouse

**Configuration:**
- Warehouse: `de_wh`
- Size: X-Small
- Auto-suspend: 60 seconds
- Auto-resume: enabled

**Billing Basis:**
- Snowflake documents X-Small standard warehouses as consuming 1 credit per hour while running.
- Snowflake bills warehouse usage per second, with a 60-second minimum each time the warehouse starts.

**Observed Runtime:**
- Sampled successful Airflow runs show the Snowflake load task completing in roughly 7 to 10 seconds end-to-end.

**Cost-Saving Measures:**
- The warehouse is configured with a 60-second auto-suspend threshold to limit idle runtime.
- The load step uses staged loading plus `MERGE`-based upserts so reruns update existing business keys rather than duplicating records.

**Estimated Daily Compute Usage:**
- Exact dollar cost is account-specific because Snowflake credit pricing depends on the account contract or edition.
- At the usage level, a short daily run on an X-Small warehouse is typically on the order of one minimum-billed warehouse start, assuming the warehouse is otherwise idle.

### AWS Glue (Spark)

**Configuration:**
- Region: us-east-1
- Glue version: 5.0
- Worker type: G.1X
- Worker count: 2
- Timeout: 10 minutes

**Billing Basis:**
- AWS Glue ETL jobs are billed by the second at a public list rate of $0.44 per DPU-hour, though AWS notes that pricing can vary by region.
- A G.1X worker maps to 1 DPU, so this job runs with 2 DPU total.

**Observed Runtime:**
- Sampled successful runs complete in roughly 69 to 118 seconds, which is consistent with an operational range of about 1 to 2 minutes.

**Cost-Saving Measures:**
- The Glue job only runs after successful extraction.
- The job has an explicit 10-minute timeout to limit runaway compute.
- The DAG waits for the Glue job to finish before scheduling downstream tasks.

**Estimated Per-Run Cost:**
- Using the public $0.44 per DPU-hour reference rate and a 2-DPU job, a typical successful run in the observed duration range is approximately $0.017 to $0.029 per run.
- Treat this as an approximate public-pricing estimate rather than a billed-account total.

## 2. Storage Costs

Storage cost is minimal for the current dataset size. This section focuses on storage format and operational characteristics more than precise monthly billing totals.

### AWS S3 (us-east-2)

**Configuration:**
- Region: us-east-2

**Storage Characteristics:**
- Bronze data is stored as raw JSON.
- Silver and Gold data are stored as Parquet.

**Estimated Monthly Cost:**
- For the current project scale, storage cost should remain negligible.
- Exact monthly cost depends on regional S3 Standard pricing, object count, total retained bytes, and request volume.

### Snowflake Storage

**Storage Characteristics:**
- Curated data is loaded into Snowflake warehouse tables after the S3 Gold stage.
- Snowflake manages internal storage layout and compression automatically.

**Estimated Monthly Cost:**
- Storage cost is expected to remain negligible at the current project scale.
- Exact monthly cost depends on retained compressed table volume and account-specific Snowflake pricing.

## 3. System Limits & Quotas

### Open-Meteo API

**Published Free API Limits:**
- 600 calls per minute
- 5,000 calls per hour
- 10,000 calls per day

**Mitigation:**
- The current extraction path issues a single parameterized HTTP request per run date and location.
- With the current single-location workflow, the pipeline remains comfortably below the documented free-tier limits.

### Airflow Local Concurrency

**Configuration:**
- Executor: LocalExecutor backed by PostgreSQL
- DAG-level overlap control: `max_active_runs = 1`

**Mitigation:**
- The DAG is configured to prevent overlapping runs of the same pipeline.
- This reduces local resource contention during manual reruns and historical backfills.

### AWS Account Limits

**Operational Constraint:**
- AWS Glue job concurrency and service quotas are account- and region-specific.

**Mitigation:**
- This pipeline invokes Glue synchronously and does not launch overlapping Glue tasks from the same DAG run.
- Combined with `max_active_runs = 1` on the DAG, this project avoids concurrent Glue executions from this pipeline.
