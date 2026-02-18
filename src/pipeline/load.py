"""
    CLI entry point for loading silver Parquet data into Postgres staging.

    Orchestrates: Read silver Parquet -> Connect to Postgres -> Write to raw_weather table.

    Usage:
    python -m src.pipeline.load --run-date 2026-02-01 --location Boston
"""

import pandas as pd
import argparse
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import logging
from src.pipeline.config import Project_Config
from src.pipeline.io.local import read_parquet_local

logger = logging.getLogger(__name__)

def connect_to_postgres() -> Engine:
    """
    Establishes a connection to the Postgres database using SQLAlchemy.
    
    Returns:
        sqlalchemy.engine.Engine: The connection engine.
    """
    conn_str = Project_Config.Database.connection_string()
    logger.info("Connecting to PostgreSQL...")
    engine = create_engine(conn_str)
    logger.info("Connection successful!")
    return engine
    

def load_silver_data(run_date: str, location: str, source: str) -> pd.DataFrame:
    """
    Reads the processed silver data (Parquet) for the specific run.
    
    Args:
        run_date: Date of the run in YYYY-MM-DD format
        location: Location name
        source: Data source name
        
    Returns:
        pd.DataFrame: The loaded data
    """
    
    silver_path = Project_Config.Paths.silver_path(source, run_date, location)
    file_path = f"{silver_path}/weather_data.parquet"

    logger.info(f"Loading silver data from: {file_path}")
    df = read_parquet_local(file_path)

    record_count = len(df)
    logger.info(f"Loaded {record_count} records from silver layer")
    logger.debug(f"DataFrame shape: {df.shape}, columns: {list(df.columns)}")
    
    return df

def write_to_postgres(df: pd.DataFrame, table_name: str, engine: Engine) -> None:
    """
    Writes the DataFrame to the Postgres database.
    
    Args:
        df: The dataframe to write
        table_name: The target table name in Postgres
        engine: The sqlalchemy connection engine
    """
    logger.info(f"Writing {len(df)} records to '{table_name}' table...")
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    logger.info(f"Successfully wrote {len(df)} records to PostgreSQL table '{table_name}'")

def run_load(run_date: str, location: str = "Boston", source: str = "openmeteo") -> None:
    """
    Orchestrates the loading process: Connect -> Read -> Write.
    """
    logger.info(f"Starting load for {run_date}...")
    
    engine = connect_to_postgres()
    
    df = load_silver_data(run_date, location, source)
    df['location'] = location
    df['source'] = source
    df['run_date'] = run_date
    write_to_postgres(df, 'raw_weather', engine)

    logger.info("Load completed successfully.")

def main():
    """
    CLI entry point. Parses arguments and orchestrates the load process.
    """
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    
    parser = argparse.ArgumentParser(description="Load silver Parquet data into Postgres staging")
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
    Project_Config.validate()
    run_load(args.run_date, args.location, args.source)

if __name__ == "__main__":
    main()



    