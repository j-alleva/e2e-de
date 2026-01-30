
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
import pandas as pd
from src.pipeline.config import Project_Config

#Helper Functions

#extracts bronze JSON data
def _load_bronze_data(file_path : str) -> dict:
    
    #opens file path and extracts raw JSON bronze data
    with open (file_path, "r") as f:
        LOADED_JSON = json.load(f)

    return LOADED_JSON

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

    return df

#saves normalized data to silver path
def _save_to_silver(df : pd.DataFrame,run_date : str) -> str:

    #quick validation double check

    if df.empty:
        raise ValueError("Normalized DataFrame is empty!")
    #create dir_path and file_path

    dir_path = f"{Project_Config.Paths.LOCAL_SILVER}/source=openmeteo/run_date={run_date}"

    file_path = f"{dir_path}/weather_data.parquet"
    #create directory if doesn't exist

    os.makedirs(dir_path, exist_ok = True)

    #saves silver data as parquet
    df.to_parquet(file_path, index=False)

    return file_path

#orchestration
def run_normalize(run_date : str):
    """
    Orchestrator for normalization.
    Raises exception if normalization fails
    """
    print(f"[NORMALIZE] Starting normalization for [{run_date}]")

    #identify input path
    try:
        input_path = f"{Project_Config.Paths.LOCAL_BRONZE}/source=openmeteo/run_date={run_date}/raw.json"

        #load bronze data

        raw_data = _load_bronze_data(input_path)

        #normalize bronze data

        df = _normalize_data(raw_data)

        #save normalized data to silver

        _save_to_silver(df,run_date)
        
        #establish output_path for terminal
        output_path = _save_to_silver(df,run_date)
        #confirmation messages
        print (f"JSON data from {run_date} succsessfully normalized and saved to {output_path}")
        print (f"Shape of data: {df.shape}")

        return True

    except Exception as e:
        print (f"Normalization failed {e}")
        raise e
    
#script entry point
if __name__ == "__main__":

    valid_date = "2026-01-25"

    run_normalize(valid_date)











