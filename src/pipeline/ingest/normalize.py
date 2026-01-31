
"""
Data normalization module for silver layer transformation.

Transforms validated bronze JSON into cleaned Parquet format:
- Flattens hourly weather data into tabular structure
- Adds location metadata (latitude/longitude)
- Converts timestamps to datetime objects
- Saves as partitioned Parquet to silver layer
"""

import os
import json
import logging
import pandas as pd
from src.pipeline.config import Project_Config

logger = logging.getLogger(__name__)

def _load_bronze_data(file_path : str) -> dict:
    """
    Load validated bronze JSON data.
    
    Args:
        file_path: Path to bronze JSON file
    
    Returns:
        Parsed JSON data as dictionary
    """
    logger.debug(f"Loading bronze data from: {file_path}")
    
    with open (file_path, "r") as f:
        data = json.load(f)

    logger.debug(f"Bronze data loaded successfully")
    return data


def _normalize_data(data : dict) -> pd.DataFrame:
    """
    Transform JSON to normalized DataFrame with proper data types.
    
    Args:
        data: Bronze layer JSON data
    
    Returns:
        Normalized DataFrame with hourly weather data and location columns
    """
    
    # Extract and flatten hourly data
    hourly_data = data['hourly']
    df = pd.DataFrame(hourly_data)

    # Add location metadata
    df['latitude'] = data['latitude']
    df['longitude'] = data['longitude']

    # Convert time column to datetime objects
    df['time'] = pd.to_datetime(df['time'])

    record_count = len(df)
    logger.info(f"Normalized {record_count} records for silver layer")
    logger.debug(f"DataFrame shape: {df.shape}, columns: {list(df.columns)}")

    return df


def _save_to_silver(df : pd.DataFrame,run_date : str, location: str, source: str) -> str:
    """
    Save normalized DataFrame to silver layer as Parquet.
    
    Args:
        df: Normalized DataFrame
        run_date: Date in YYYY-MM-DD format
        location: Location name
        source: Data source identifier
    
    Returns:
        Path to saved Parquet file
    
    Raises:
        ValueError: If DataFrame is empty
    """
    # Ensure DataFrame is not empty
    if df.empty:
        logger.error("Normalized DataFrame is empty!")
        raise ValueError("Normalized DataFrame is empty!")
    
    # Generate partitioned path
    dir_path = Project_Config.Paths.silver_path(source, run_date, location)
    file_path = f"{dir_path}/weather_data.parquet"

    # Create directory if not already existing and save
    os.makedirs(dir_path, exist_ok = True)
    logger.debug(f"Created directory: {dir_path}")

    df.to_parquet(file_path, index=False)
    logger.info(f"Wrote Parquet to: {file_path}")

    return file_path


def run_normalize(run_date : str, location : str = "Boston", source: str = "openmeteo") -> bool:
    """
    Orchestrate normalization: load bronze, transform, save to silver.
    
    Args:
        run_date: Date in YYYY-MM-DD format
        location: Location name. Defaults to 'Boston'.
        source: Data source identifier. Defaults to 'openmeteo'.
    
    Returns:
        True if normalization completes successfully
    
    Raises:
        ValueError: If DataFrame is empty or data invalid
        Exception: For other unexpected errors during normalization
    """

    
    try:
        logger.info(f"Starting normalization: source={source}, location={location}, run_date={run_date}")

        # Build input path
        input_path = f"{Project_Config.Paths.bronze_path(source, run_date, location)}/raw.json"

        # Load -> normalize -> save
        raw_data = _load_bronze_data(input_path)
        df = _normalize_data(raw_data)
        _save_to_silver(df,run_date,location,source)
        
        logger.info(f"Normalization completed successfully")
        return True

    except Exception as e:
        logger.error(f"Normalization failed: {e}")
        raise 
    












