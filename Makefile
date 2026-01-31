.PHONY: help ingest clean test lint

help:  ## Show available commands
    @echo "Available commands:"
    @echo "  make ingest RUN_DATE=YYYY-MM-DD LOCATION=Boston  - Run ingestion pipeline"
    @echo "  make clean                                         - Remove data directories"
    @echo "  make test                                          - Run tests (placeholder)"
    @echo "  make lint                                          - Run linter (placeholder)"

ingest:  ## Run the ingestion pipeline
    @python -m src.pipeline.run --run-date $(RUN_DATE) --location $(LOCATION)

clean:  ## Remove bronze and silver data
    @rm -rf data/bronze data/silver
    @echo "Cleaned data directories"

test:  ## Run tests (placeholder for future)
    @echo "Tests not yet implemented"

lint:  ## Run linter (placeholder for future)
    @echo "Linting not yet configured"