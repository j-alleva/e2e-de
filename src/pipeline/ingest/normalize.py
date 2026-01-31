
"""

normalize.py
------------
Normalizes Raw JSON (Bronze) -> Cleaned Parquet (Silver)

Responsibilities:
1. Load validated Bronze JSON da
2. Flattens hourly data into a tabular pandas dataframe
3. Enforce data types (Converting strings to datetime objects)
4. Save the result as a partioned parquet file in the Silver layer

"""

import os
import json
import logging
import pandas as pd
from src.pipeline.config import Project_Config

#Helper Functions

logger = logging.getLogger(__name__)
#extracts bronze JSON data
def _load_bronze_data(file_path : str) -> dict:
    
    logger.debug(f"Loading bronze data from: {file_path}")
    #opens file path and extracts raw JSON bronze data
    with open (file_path, "r") as f:
        data = json.load(f)

    logger.debug(f"Bronze data loaded successfully")
    return data

#normalize bronze JSON data
def _normalize_data(data : dict) -> pd.DataFrame:

    #extract hourly data

    hourly_data = data['hourly']

    #Convert hourly data to dataframe

    df = pd.DataFrame(hourly_data)

    #Add latitude and longitude columns

    df['latitude'] = data['latitude']
    df['longitude'] = data['longitude']

    #Convert time column to datetime objects

    df['time'] = pd.to_datetime(df['time'])

    record_count = len(df)
    logger.info(f"Normalized {record_count} records for silver layer")
    logger.debug(f"DataFrame shape: {df.shape}, columns: {list(df.columns)}")

    return df

#saves normalized data to silver path
def _save_to_silver(df : pd.DataFrame,run_date : str, location: str, source: str) -> str:

    #quick validation double check

    if df.empty:
        logger.error("Normalized DataFrame is empty!")
        raise ValueError("Normalized DataFrame is empty!")
    #create dir_path and file_path

    dir_path = Project_Config.Paths.silver_path(source, run_date, location)

    file_path = f"{dir_path}/weather_data.parquet"
    #create directory if doesn't exist

    os.makedirs(dir_path, exist_ok = True)
    logger.debug(f"Created directory: {dir_path}")

    #saves silver data as parquet
    df.to_parquet(file_path, index=False)

    logger.info(f"Wrote Parquet to: {file_path}")
    return file_path

#orchestration
def run_normalize(run_date : str, location : str = "Boston", source: str = "openmeteo"):
    """
    Orchestrator for normalization.
    Raises exception if normalization fails
    """

    #identify input path
    try:
        logger.info(f"Starting normalization: source = {source}, location = {location}, run_date={run_date}")

        input_path = f"{Project_Config.Paths.bronze_path(source, run_date, location)}/raw.json"

        #load bronze data

        raw_data = _load_bronze_data(input_path)

        #normalize bronze data

        df = _normalize_data(raw_data)
        
        #establish output_path for terminal
        output_path = _save_to_silver(df,run_date,location,source)
        #confirmation messages
        
        logger.info(f"Normalization completed successfully")

        return True

    except Exception as e:
        logger.error(f"Normalization failed {e}")
        raise 
    












