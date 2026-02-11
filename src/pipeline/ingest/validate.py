
"""
Data validation module for bronze layer ingestion.

Validates raw JSON data from API before normalization:
- Schema validation (required keys present)
- Data quality checks (non-empty, parseable timestamps)
- Duplicate detection on natural key (location + timestamp)
"""

import json
import os
import logging 
from datetime import datetime
from src.pipeline.config import Project_Config
from src.pipeline.io.local import read_json_local

logger = logging.getLogger(__name__)

def _validate_schema(data:dict) -> bool:
    """
    Validate presence of required top-level keys.
    
    Args:
        data: JSON data dictionary
    
    Returns:
        True if all required keys present
    
    Raises:
        ValueError: If any required keys are missing
    """

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
    """
    Validate data quality: non-empty, parseable timestamps, no duplicates.
    
    Args:
        data: JSON data dictionary
    
    Returns:
        True if all quality checks pass
    
    Raises:
        ValueError: If data is empty, timestamps unparseable, or duplicates found
    """
    hourly = data["hourly"]

    # Check hourly data is not empty
    if not hourly.get("time") or len(hourly["time"]) == 0:
        logger.error("Hourly data is empty")
        raise ValueError(f"Dataset is empty")
    
    record_count = len(hourly["time"])
    logger.debug(f"Validating {record_count} records")
    
    # Validate timestamp format
    try:
        datetime.fromisoformat(hourly["time"][0])
        logger.debug(f"Timestamp format validated: {hourly['time'][0]}")
    except Exception as e:
        logger.error(f"Timestamp not parseable: {hourly['time'][0]}")
        raise ValueError(f"Timestamp is not parseable: {e}")
    
    # Check for duplicates on natural key (location + timestamp)
    latitude = data['latitude']
    longitude = data['longitude']
    timestamps = hourly['time']

    natural_keys = [(latitude, longitude, ts) for ts in timestamps]
    unique_keys = set(natural_keys)

    if len(unique_keys) != len(natural_keys):
        duplicate_count = len(natural_keys) - len(unique_keys)
        logger.error(f"Found {duplicate_count} duplicate records on natural key (location + timestamp)")
        raise ValueError(f"Data contains {duplicate_count} duplicates on natural key")
    
    logger.debug(f"Natural key check passed: {len(natural_keys)} unique records")
    
    logger.info(f"Data quality validation passed ({record_count} records, 0 duplicates)")
    return True
    
def validate_bronze_file(file_path : str) -> bool:
    """
    Orchestrate full validation: load, schema check, quality check.
    
    Args:
        file_path: Path to bronze JSON file
    
    Returns:
        True if validation passes
    
    Raises:
        FileNotFoundError: If file does not exist
        ValueError: If validation fails (schema or quality issues)
    """
    try:
        logger.info(f"Starting validation: {file_path}")

        
        data = read_json_local(file_path)
        _validate_schema(data)
        _validate_data_quality(data)

        logger.info(f"Data successfully passed validation! Suitable for normalization")
        return True

    except Exception:

        raise 
    
