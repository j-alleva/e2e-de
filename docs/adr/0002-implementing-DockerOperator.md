# ADR 0002: Executing Tasks via DockerOperator

## Decision
We chose the DockerOperator to execute the daily weather ingestion job instead of Airflow's native PythonOperator or BashOperator

## Rationale
DockerOperator is better than PythonOperator for this project because:
- **Environment Isolation**: By spinning up an ephemeral container, Airflow does not need to manage the ingestion script's specific dependencies (like boto3 or requests), keeping the Airflow worker lightweight and the ingestion script 100% decoupled and portable.

## Trade-offs
We lose the simplicity of native Airflow variable injection (requiring explicit 
host path mounts, network configuration, and pre-reading `.env` via `dotenv_values`), 
but we accept this because:
- The ingestion job is independently testable and runnable outside Airflow (`make ingest-s3`)
- Dependency conflicts between Airflow and the pipeline are eliminated entirely
- The container is already built, tested, and CI validated, Airflow just calls it

## Status
Accepted. Implemented. Validated.