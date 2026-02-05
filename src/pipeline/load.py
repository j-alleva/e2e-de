import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine
import logging
from src.pipeline.config import Project_Config

logger = logging.getLogger(__name__)

def connect_to_postgres() -> Engine:
    """
    Establishes a connection to the Postgres database using SQLAlchemy.
    
    Returns:
        sqlalchemy.engine.Engine: The connection engine.
    """
    conn_str = Project_Config.Database.connection_string()
    logger.info(f"Connecting to PostgreSQL...")
    engine = create_engine(conn_str)
    logger.info(f"Connection successful!")
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
    df = pd.read_parquet(file_path)

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

if __name__ == "__main__":

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    run_load(run_date="2026-02-01")



    