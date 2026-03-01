.PHONY: help down ingest ingest-s3 schema load warehouse queries clean airflow-init airflow-up airflow-down build

help:
	@echo "Available: down ingest ingest-s3 schema load warehouse queries clean airflow-init airflow-up airflow-down build"

down: ## Stop Docker
	@docker compose down

build: ## Build de-ingest Docker image
	@docker build -t de-ingest .

airflow-init: ## Initialize Airflow database and user
	@docker compose up airflow-init

airflow-up: ## Start Airflow and Postgres Warehouse
	@docker compose up -d

airflow-down: ## Stop Airflow and Postgres
	@docker compose down -v

ingest: ## Run Dockerized Pipeline (Extract + Write Local)
	@docker run --rm --env-file .env \
		-v $(CURDIR)/data:/app/data \
		de-ingest --run-date $(RUN_DATE) --location $(LOCATION)

ingest-s3: ## Run Dockerized Pipeline (Extract + Write Local + S3 Upload)
	@docker run --rm --env-file .env \
		-v $(CURDIR)/data:/app/data \
		de-ingest --run-date $(RUN_DATE) --location $(LOCATION) --write-s3

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