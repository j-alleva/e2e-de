# ADR 0001: Containerizing the Ingestion Pipeline with Docker

## Decision
We chose Docker containerization for the Python ingestion script (Python 3.13-slim base image, single-stage build).

## Rationale
Docker is better than bare Python venv for this project because:
- **Environment Parity**: Code runs identically on dev and CI.
- **Dependency Locking**: Pinned versions in requirements.txt + frozen OS prevent local dependency failures.

## Trade-offs
We lose negligible local iteration speed, but we accept that because the orchestration clarity and environment consistency across dev/CI/production outweighs the overhead.

## Status
Accepted. Implemented. Validated.