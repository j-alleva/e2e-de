
"""

validate.py: Ensures raw JSON data passed basic validation checks

Capabilities 

1. Schema validation: Ensure JSON data contains the correct requried top level keys
2. Data quality: Checks if arrays are empty, if timestamps are parseable
3. Integrity: Ensures lack of duplicates 

"""

import json
import os
from datetime import datetime
from src.pipeline.config import Project_Config

#loads raw data
def _load_json(file_path:str) -> dict:

    #Ensures file path exists, raises FileNotFoundError
    if not os.path.exists(file_path):
        raise FileNotFoundError(f"File not found")
    
    #Pulls json data from the file path, and returns
    with open(file_path, "r") as f:

        LOADED_JSON = json.load(f)

    return LOADED_JSON

#validates required keys
def _validate_schema(data:dict) -> bool:

    #list of required top level keys for open meteo API pull
    REQUIRED_KEYS = [
        "latitude",
        "longitude",
        "generationtime_ms",
        "utc_offset_seconds",
        "timezone",
        "timezone_abbreviation",
        "elevation",
        "hourly_units",
        "hourly"
    ]

    #ensures data has all required keys, raises ValueError if top level key is missing
    for key in REQUIRED_KEYS:
        if not key in data:
            raise ValueError(f"Data is missing required top level key")
        
    return True


def _validate_data_quality(data:dict) -> bool:

    #pulls hourly dataset
    hourly = data["hourly"]

    #ensure hourly data is not empty
    if not hourly.get("time") or len(hourly["time"]) == 0:
        raise ValueError(f"Dataset is empty")
    
    #ensure timestamps are parseable
    try:
        datetime.fromisoformat(hourly["time"][0])
    except Exception:
        raise ValueError("Timestamp is not parseable")
    
    #ensure hourly data contains no duplicates
    if len(set(hourly["time"])) != len(hourly["time"]):
        raise ValueError("Hourly data contains duplicates")
    
def validate_bronze_file(file_path : str) -> bool:

    """
    Orchestration for validation

    1. Loads JSON File
    2. Checks validitiy of schema
    3. Checks quality of schema; ensures not empty and lack of duplicates

    Returns error/success message based on validation process

    """
    try:

        #loads raw JSON data
        data = _load_json(file_path)

        #validates schema

        _validate_schema(data)

        #validates data quality

        _validate_data_quality(data)

        print(f"Data succsessfully passed validation! Suitable for normalization")

    #raises error if validation failed at any point
    except Exception as e:

        print (f"Error raised, Validation failed {e}")

        raise e
    
