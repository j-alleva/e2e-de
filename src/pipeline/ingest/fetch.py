
"""
Data extraction module for ingestion pipeline.

Fetches raw weather data from Open-Meteo API and saves to bronze layer
with partitioning by source, run_date, and location.

"""

import requests
import logging
from datetime import datetime, timezone
from src.pipeline.config import Project_Config
from src.pipeline.io.local import save_json_local
from src.pipeline.io.s3 import S3Client

logger = logging.getLogger(__name__)

def _build_url(location: str, run_date :str ) -> str:
    """
    Build API URL with location coordinates and date parameters.

    Args:
        location: Location name (e.g., 'Boston')
        run_date: Date in YYYY-MM-DD format

    Returns:
        Complete API URL with query parameters
    """
    base_url = Project_Config.API.get_open_meteo_url(location)

    url = f"{base_url}&start_date={run_date}&end_date={run_date}"
    logger.debug(f"Built API URL: {url}")
    return url

def _fetch_from_api(url:str) -> dict:
    """
    Fetch weather data from API endpoint.

    Args:
        url: Complete API URL with parameters

    Returns: 
        JSON response data as dictionary

    Raises:
        requests.exceptions.RequestException: If HTTP request fails
    """
    logger.info("Sending GET request to API")
    response = requests.get(url)
    response.raise_for_status()

    data = response.json()

    record_count = len(data.get('hourly',{}).get('time', []))
    logger.info(f"Fetched {record_count} records from API")

    return data

def _save_to_bronze(data:dict, run_date:str, location:str,source:str, write_to_s3: bool = False) -> str:
    """
    Save raw API data to bronze layer with metadata.

    Args:
        data: Raw JSON data from API
        run_date: Date in YYYY-MM-DD format
        location: Location name
        source: Data source identifier

    Returns:
        Path to saved JSON file
    """
    # Add ingestion metadata
    data["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    data["source"] = source

    # Use Config to get the directory path (e.g., data/bronze/source=openmeteo/...)
    dir_path = Project_Config.Paths.bronze_path(source, run_date, location)
    file_path = f"{dir_path}/raw.json"

    # Call our centralized I/O module instead of handling it here
    save_json_local(data, file_path)

    if write_to_s3:
        logger.info(f"Uploading bronze file to S3: {file_path}")
        s3 = S3Client()
        #No s3 key auto-mirrors local folder structure 
        s3.upload_file(local_path = file_path, s3_key=None)

    return file_path

def _run_fetch(run_date:str, location:str,source:str,write_to_s3 : bool = False) -> None:
    """
    Orchestrate fetch process: build URL, fetch data, save to bronze.
    
    Args:
        run_date: Date in YYYY-MM-DD format
        location: Location name (e.g., 'Boston')
        source: Data source identifier (e.g., 'openmeteo')
    
    Raises:
        requests.exceptions.RequestException: If API request fails
        Exception: For other unexpected errors during fetch
    """
    try:
        logger.info(f"Starting fetch: source={source}, location={location}, run_date={run_date}")
        
        url = _build_url(location, run_date)
        data = _fetch_from_api(url)
        _save_to_bronze(data, run_date, location, source, write_to_s3=write_to_s3)

    except Exception as e:
        logger.error(f"Error during fetch: {e}")
        raise
    





