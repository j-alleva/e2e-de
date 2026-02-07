.PHONY: help up down ingest schema load warehouse queries clean

help:
	@echo "Available: up, down, ingest, schema, load, warehouse, queries, clean"

up:  ## Start Docker
	@docker compose up -d

down: ## Stop Docker
	@docker compose down

ingest: ## Run Python Pipeline (Extract + Load Raw)
	@python -m src.pipeline.run --run-date $(RUN_DATE) --location $(LOCATION)

schema: ## Create Postgres schema (dimensions, facts, staging)
	@docker exec -i de_postgres psql -U admin -d warehouse < sql/postgres/01_create_tables.sql

load: schema ## Load silver Parquet into raw_weather
	@python -m src.pipeline.load --run-date $(RUN_DATE) --location $(LOCATION)

warehouse: ## Populate fact and dimension tables from raw_weather
	@echo "Populating Fact/Dims..."
	@docker exec -i de_postgres psql -U admin -d warehouse < sql/postgres/02_populate_tables.sql

queries: ## Run All Analytics
	@for f in sql/queries/*.sql; do \
		echo "Running $$f..."; \
		docker exec -i de_postgres psql -U admin -d warehouse < $$f; \
	done

clean:
	@rm -rf data/bronze data/silver