# ADR 0004: Enforcing Data Quality via dbt YAML Tests

## Decision
We chose to use dbt's native YAML tests (`unique`, `not_null`, `relationships`) as the primary enforcement mechanism, while Snowflake constraints remain informational metadata.

## Rationale
dbt testing is better for this pipeline because:
- **Enforcement**: Snowflake PK/FK constraints are informational in this setup, so duplicate prevention is enforced through idempotent MERGE logic in the load layer and validated again via dbt tests.
- **Testing as Code**: Quality rules live in version control alongside the SQL models they protect, and are currently executed in local/interactive runs; CI compile validation is in place, with CI test execution planned.

## Trade-offs
We consume slightly more Snowflake compute credits during a `dbt build` run to execute the test queries, but we accept that because catching bad data before it reaches the BI/dashboarding layer preserves business trust.

## Status
Accepted. Implemented. Locally validated. CI test enforcement pending.