# Cloud Infrastructure Documentation

## 1. Storage Architecture (S3)
**Bucket Name:** `de-portfolio-jack-99239`
**Region:** `us-east-2`

### Data Lake Layout
We adhere to a **Hive-style partitioning strategy** to optimize for future query performance (e.g. Spark) and logical separation of concerns.

* **Bronze Layer (`/data/bronze`)**
    * **Format:** Raw JSON (immutable).
    * **Partitioning:** `source={source_name}/run_date={YYYY-MM-DD}/location={city}/`
    * **Purpose:** Archival and replayability.

* **Silver Layer (`/data/silver`)**
    * **Format:** Snappy-compressed Parquet.
    * **Partitioning:** Mirrors Bronze.
    * **Purpose:** Analytics-ready data with enforced schema and types.

---

## 2. Security (IAM Least Privilege)
To strictly limit blast radius and adhere to security best practices, we replaced default admin keys with a scoped service account.

### Policy: `Portfolio-S3-Strict-Access`
* **Principle:** The programmatic user is restricted *only* to the portfolio bucket.
* **Allowed Actions:**
    * `s3:PutObject` (Ingestion)
    * `s3:GetObject` (Transformation)
    * `s3:ListBucket` (Validation)
* **Resource Constraints:**
    * `arn:aws:s3:::de-portfolio-jack-99239` (Bucket level)
    * `arn:aws:s3:::de-portfolio-jack-99239/*` (Object level)

### Future Roadmap (Gold Layer)
* **Status:** Planned
* **Purpose:** Business-level aggregates (e.g., Weekly Average Temperature).
* **Format:** Delta Lake or Parquet.
* **Transformation:** AWS Glue Spark (Block 6) → dbt for analytics marts (Block 8) on Snowflake.