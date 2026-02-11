"""
Local filesystem I/O operations for data pipeline.

Handles reading and writing data to local storage with support for:
- Bronze layer (raw JSON)
- Silver layer (Parquet)
- Path validation and directory creation
"""

import os
import json
import logging
import pandas as pd

logger = logging.getLogger(__name__)

def save_json_local(data: dict, file_path: str) -> str:
    """Saves a dictionary to a local JSON file, creating directories if needed."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, "w") as f:
        json.dump(data, f)
    logger.info(f"Successfully saved JSON to local: {file_path}")
    return file_path

def save_parquet_local(df: pd.DataFrame, file_path: str) -> str:
    """Saves a DataFrame to a local Parquet file, creating directories if needed."""
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_parquet(file_path, index=False)
    logger.info(f"Successfully saved Parquet to local: {file_path}")
    return file_path

def read_json_local(file_path: str) -> dict:
    """Reads a local JSON file and returns a dictionary."""
    if not os.path.exists(file_path):
        logger.error(f"JSON file not found: {file_path}")
        raise FileNotFoundError(f"No file at {file_path}")
    
    with open(file_path, "r") as f:
        data = json.load(f)
    logger.debug(f"Successfully read JSON from local: {file_path}")
    return data

def read_parquet_local(file_path: str) -> pd.DataFrame:
    """Reads a local Parquet file and returns a DataFrame."""
    if not os.path.exists(file_path):
        logger.error(f"Parquet file not found: {file_path}")
        raise FileNotFoundError(f"No file at {file_path}")
    
    df = pd.read_parquet(file_path)
    logger.debug(f"Successfully read Parquet from local: {file_path}")
    return df
