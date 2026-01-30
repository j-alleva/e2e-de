
"""
fetch.py

Responsible for extracting data from API into raw JSON and placing into partioned data 

"""

import os
import requests
import json
from datetime import datetime, timezone
from src.pipeline.config import Project_Config

#helper functions

def _build_url(location: str, run_date :str ) -> str:

    #extracts api url from config file
    base_url = Project_Config.API.get_open_meteo_url(location)

    #adjust url for our specfic pull date, avoids open meteo default 7 day pull

    url = f"{base_url}&start_date={run_date}&end_date={run_date}"

    return url

def _fetch_from_api(url:str) -> str:

    #sends http get request to API
    response = requests.get(url)

    #ensures success of http get
    response.raise_for_status()

    #returns request as raw json
    return response.json()

def _save_to_bronze(data:dict, run_date:str, location:str,source:str) -> str:

    #adds metadata of time and source
    data["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
    data["source"] = source

    #defines file paths
    dir_path = Project_Config.Paths.bronze_path(source, run_date, location)
    file_path = f"{dir_path}/raw.json"

    #creates directories
    os.makedirs(dir_path, exist_ok = True)

    #write json dump
    with open(file_path,"w") as f:
        json.dump(data,f)

    #return filepath
    return file_path

def _run_fetch(run_date:str, location:str,source:str):

    try:
        #pulls url from config
        url = _build_url(location, run_date)

        #fetches our JSON data from the API
        data = _fetch_from_api(url)

        #saves the raw JSON to our chosen bronze directory
        file_path = _save_to_bronze(data, run_date, location,source)

        #success check
        print (f"Success! Raw JSON data written to {file_path}")

    #error handling if data fetch sends an error
    except Exception as e:

        print(f"X! Error during fetch: {e}")
        raise e
    





