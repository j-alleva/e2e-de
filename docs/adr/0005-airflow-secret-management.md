# ADR 0005: Hybrid Secret Management via Airflow UI Connections

## Decision
We chose a hybrid secret management strategy: using a `.env` file for local Docker container injection (Python ingestion, dbt) while relying strictly on Airflow UI Connections (stored in its internal Postgres DB) for cloud operators (`GlueJobOperator`, `SnowflakeOperator`).

## Rationale
AWS Secret Access Keys frequently contain special characters (like `/`, `+`, or `=`). When configured via `.env` URL strings, Airflow's URI parser often misinterprets these characters, leading to silent connection failures or "NoRegionError" exceptions. By utilizing the native Airflow UI for these specific cloud handshakes, we bypass URL encoding issues entirely and ensure robust, native credential parsing.

## Trade-offs
We lose the simplicity of having a single `.env` file act as the absolute source of truth for all secrets, increasing local onboarding complexity by requiring UI configuration steps. However, we accept this because it mimics enterprise production environments where different components rely on distinct, isolated secret stores.

## Status
Accepted. Implemented. Validated.