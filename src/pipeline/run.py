
"""
CLI entry point for data ingestion pipeline.

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

def run_pipeline(run_date: str, location: str = "Boston", source: str = "openmeteo", write_to_s3: bool = False) -> bool:
    """
    Orchestrate the full ingestion pipeline: fetch -> validate -> normalize.
    
    Args:
        run_date: Date to process in YYYY-MM-DD format
        location: Location name. Defaults to 'Boston'.
        source: Data source identifier. Defaults to 'openmeteo'.
        write_to_s3: If true, upload bronze/silver files to S3. Defaults to False
    
    Returns:
        True if pipeline completes successfully
    
    Raises:
        Exception: Any error during fetch, validation, or normalization causes sys.exit(1)
    """
    logger.info(f"="*60)
    logger.info(f"Starting Pipeline: {source} | {location} | {run_date}")
    logger.info(f"="*60)
    
    try:
        logger.info(f"[1/3] FETCH: Retrieving data from API...")
        _run_fetch(run_date, location, source, write_to_s3=write_to_s3)

        logger.info(f"[2/3] VALIDATE: Checking data quality...")
        bronze_path = f"{Project_Config.Paths.bronze_path(source, run_date, location)}/raw.json"
        validate_bronze_file(bronze_path)

        logger.info(f"[3/3] NORMALIZE: Transforming to silver layer...")
        run_normalize(run_date, location, source, write_to_s3=write_to_s3)

        logger.info(f"="*60)
        logger.info(f"Pipeline completed successfully!")
        logger.info(f"="*60)

        return True
    
    except Exception as e:
        logger.error(f"="*60)
        logger.error(f"Pipeline failed: {e}")
        logger.error(f"="*60)
        sys.exit(1)

def main():
    """
    Parse CLI arguments and execute the pipeline.
    
    Sets up logging to console and file, parses command-line arguments,
    validates configuration, and runs the pipeline.
    """
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

    parser.add_argument(
        "--write-s3",
        action="store_true",
        help="Upload data to s3 (default: False)"
    )

    args = parser.parse_args()

    logger.info(f"CLI arguments parsed: run_date={args.run_date}, location={args.location}, source={args.source}")

    Project_Config.validate()

    run_pipeline(args.run_date, args.location, args.source,write_to_s3 = args.write_s3)

if __name__ == "__main__":
    main()






        
