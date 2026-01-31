"""
run.py
------
CLI entry point for the data ingestion pipeline.
Orchestrates fetch -> validate -> normalize.

Usage:
    python -m src.pipeline.run --run-date 2026-01-25 --location Boston
"""

import argparse
import sys
import logging
from src.pipeline.ingest.fetch import _run_fetch
from src.pipeline.ingest.validate import validate_bronze_file
from src.pipeline.ingest.normalize import run_normalize
from src.pipeline.config import Project_Config

logger = logging.getLogger(__name__)

def run_pipeline(run_date: str, location: str = "Boston", source: str = "openmeteo"):
    """
    Orchestrates the full ingestion pipeline.
    
    Args:
        run_date: Date to process (YYYY-MM-DD format)
        location: Location name (e.g., Boston)
        source: Data source identifier (default: openmeteo)
    """
    logger.info(f"{'='*60}")
    logger.info(f"Starting Pipeline: {source} | {location} | {run_date}")
    logger.info(f"{'='*60}")
    
    try:
        # Step 1: Fetch raw data
        logger.info("[1/3] FETCH: Retrieving data from API...")
        _run_fetch(run_date, location, source)
        
        # Step 2: Validate bronze data
        logger.info("[2/3] VALIDATE: Checking data quality...")
        bronze_path = f"{Project_Config.Paths.bronze_path(source, run_date, location)}/raw.json"
        validate_bronze_file(bronze_path)
        
        # Step 3: Normalize to silver
        logger.info("[3/3] NORMALIZE: Transforming to silver layer...")
        run_normalize(run_date, location, source)
        
        logger.info(f"{'='*60}")
        logger.info(f"Pipeline completed successfully!")
        logger.info(f"{'='*60}")
        
        return True
        
    except Exception as e:
        logger.error(f"{'='*60}")
        logger.error(f"Pipeline failed: {e}")
        logger.error(f"{'='*60}")
        sys.exit(1)


def main():

    """Parse CLI arguments and run the pipeline."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('pipeline.log')
        ]
    )

    
    parser = argparse.ArgumentParser(
        description="Run the data ingestion pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m src.pipeline.run --run-date 2026-01-25 --location Boston
  python -m src.pipeline.run --run-date 2026-01-25 --location Boston --source openmeteo
        """
    )
    
    parser.add_argument(
        "--run-date",
        required=True,
        help="Date to process in YYYY-MM-DD format"
    )
    
    parser.add_argument(
        "--location",
        default="Boston",
        help="Location name (default: Boston)"
    )
    
    parser.add_argument(
        "--source",
        default="openmeteo",
        help="Data source identifier (default: openmeteo)"
    )
    
    args = parser.parse_args()
    
    logger.info(f"CLI arguments parsed: run_date={args.run_date}, location={args.location}, source={args.source}")

    # Validate config before running
    Project_Config.validate()
    
    # Run the pipeline
    run_pipeline(args.run_date, args.location, args.source)


if __name__ == "__main__":
    main()