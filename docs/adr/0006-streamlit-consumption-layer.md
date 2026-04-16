# ADR 0002: Streamlit for the Consumption Layer

## Decision
We chose Streamlit as the stakeholder facing consumption layer, connecting directly to Snowflake via `snowflake-connector-python` and utilizing `@st.cache_data` for performance.

## Rationale
Streamlit is superior to a traditional BI tool or a custom React/Node app for this project because:
- **Python-Native:** Keeps the entire repository (from Airflow to ingestion to frontend) in a single unified language.
- **Direct Governance:** We can programmatically enforce that the dashboard only queries the finalized `mart_daily_weather_summary` dbt mart, abstracting the staging layers from the end user.
- **Cost Optimization:** Snowflake compute can be expensive. Wrapping the data fetch in `@st.cache_data` ensures that user UI interactions (like filtering by location or date) happen in memory, rather than triggering a new warehouse execution for every click.

## Trade-offs
We trade the advanced visual customization of a full stack web framework for rapid, data centric prototyping. We accept this because the core objective is demonstrating governed data delivery and full stack data engineering, not frontend design.

## Status
Accepted. Implemented. Validated.