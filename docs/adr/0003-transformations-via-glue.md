# ADR 0003: Silver to Gold Transformations via AWS Glue (PySpark)

## Decision
We chose a serverless AWS Glue PySpark job for transforming and loading our curated S3 Gold layer.

## Rationale
PySpark on AWS Glue is better than a simple Pandas script or local SQL transformation for this project because:
- **Distributed Computing**: It serves as an explicit demonstration of enterprise standard distributed computing skills. While applying a distributed big data framework to a small weather dataset is intentional overengineering, it allows us to prove competency in production data lake patterns.

## Trade-offs
We lose the simplicity, low cost, and rapid execution times of running a lightweight Python transformation, but we accept that because the primary goal of this decision is to showcase ability with production grade big data tooling and cloud native execution environments.

## Status
Accepted. Implemented. Validated.