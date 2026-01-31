
"""

validate.py: Ensures raw JSON data passed basic validation checks

Capabilities 

1. Schema validation: Ensure JSON data contains the correct requried top level keys
2. Data quality: Checks if arrays are empty, if timestamps are parseable
3. Integrity: Ensures lack of duplicates 

"""

import json
import os
import logging 
from datetime import datetime
from src.pipeline.config import Project_Config

logger = logging.getLogger(__name__)

#loads raw data
def _load_json(file_path:str) -> dict:

    #Ensures file path exists, raises FileNotFoundError
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        raise FileNotFoundError(f"File not found")
    
    #Pulls json data from the file path, and returns
    with open(file_path, "r") as f:

        data = json.load(f)

    logger.debug(f"Loaded JSON from: {file_path}")
    return data

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
    missing_keys = []
    for key in REQUIRED_KEYS:
        if key not in data:
            missing_keys.append(key)
    
    if missing_keys:
        logger.error(f"Missing required keys: {', '.join(missing_keys)}")
        raise ValueError(f"Data is missing required keys: {','.join(missing_keys)}")
        
    logger.info(f"Schema validation completed ({len(REQUIRED_KEYS)} keys validated)")
    return True


def _validate_data_quality(data:dict) -> bool:

    #pulls hourly dataset
    hourly = data["hourly"]

    #ensure hourly data is not empty
    if not hourly.get("time") or len(hourly["time"]) == 0:
        logger.error("Hourly data is empty")
        raise ValueError(f"Dataset is empty")
    
    record_count = len(hourly["time"])
    logger.debug(f"Validating {record_count} records")
    
    #ensure timestamps are parseable
    try:
        datetime.fromisoformat(hourly["time"][0])
        logger.debug(f"Timestamp format validated: {hourly['time'][0]}")
    except Exception as e:
        logger.error(f"Timestamp not parseable: {hourly['time'][0]}")
        raise ValueError(f"Timestamp is not parseable: {e}")
    
    #ensure hourly data contains no duplicates
    unique_count = len(set(hourly["time"]))
    if unique_count != record_count:
        duplicate_count = record_count - unique_count
        logger.error(f"Found {duplicate_count} duplicate timestamps")
        raise ValueError(f"Hourly data contains {duplicate_count} duplicates")
    
    logger.info(f"Data quality validation passed ({record_count} records, 0 duplicates)")
    return True
    
def validate_bronze_file(file_path : str) -> bool:

    """
    Orchestration for validation

    1. Loads JSON File
    2. Checks validitiy of schema
    3. Checks quality of schema; ensures not empty and lack of duplicates

    Returns error/success message based on validation process

    """
    try:
        logger.info(f"Starting validation: {file_path}")

        #loads raw JSON data
        data = _load_json(file_path)

        #validates schema

        _validate_schema(data)

        #validates data quality

        _validate_data_quality(data)

        logger.info(f"Data successfully passed validation! Suitable for normalization")
        return True

    #raises error if validation failed at any point
    except Exception as e:

        raise 
    
