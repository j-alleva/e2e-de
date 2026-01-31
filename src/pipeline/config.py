
"""
config file for python ingestion task

Capabilities 
1. Load environment variables through load dotenv
2. Modular class hierarchy for configuration variables
3. Robust validation to ensure presence of environment variables

"""

#import os and dotenv library, and import load_dotenv for access of environment variables
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class Project_Config:

    """
    Configuration class for accessing of API and ENV variables
    Contains validation function
    """
    LOCATION_LOOKUP = {
        "Boston" : {"latitude": 42.3601, "longitude": -71.0589}
    }
    class Paths:
        #access data lake layer variables
        LOCAL_BRONZE = os.getenv("LOCAL_BRONZE_PATH")
        LOCAL_SILVER = os.getenv("LOCAL_SILVER_PATH")
        
        @classmethod
        def bronze_path(cls, source: str, run_date: str, location:str = None) -> str:
            path = f"{cls.LOCAL_BRONZE}/source={source}/run_date={run_date}"
            if location:
                path += f"/location={location}"
            logger.debug(f"Generated bronze path: {path}")
            return path
        
        @classmethod
        def silver_path(cls, source: str, run_date: str, location:str = None) -> str:
            path = f"{cls.LOCAL_SILVER}/source={source}/run_date={run_date}"
            if location:
                path += f"/location={location}"
            logger.debug(f"Generated silver path: {path}")
            return path
        
    class API:
        #access api url variable
        OPEN_METEO_URL_TEMPLATE = os.getenv("OPEN_METEO_URL_TEMPLATE")

        @classmethod
        def get_open_meteo_url(cls,location : str) -> str:
            coords = Project_Config.LOCATION_LOOKUP[location]
            url = cls.OPEN_METEO_URL_TEMPLATE.format(lat=coords["latitude"],lon=coords["longitude"])
            logger.debug(f"Generated API URL for {location}: {url}")
            return url

    @classmethod
    def validate(cls):

        #Validates presence of critical env variables
        #Raises value error if any variables are missing
        validateCheck = [
            ("LOCAL_BRONZE_PATH",cls.Paths.LOCAL_BRONZE),
            ("LOCAL_SILVER_PATH",cls.Paths.LOCAL_SILVER),
            ("OPEN_METEO_URL_TEMPLATE",cls.API.OPEN_METEO_URL_TEMPLATE)
        ]

        missing = []
        for name,value in validateCheck:
            if not value:
                missing.append(name)

        if missing:
            logger.error(f"Missing environment variables: {', '.join(missing)}")
            raise ValueError(
                f"Environment Variables are missing: {', '.join(missing)}. Please check your env file"
            )
        
        logger.info("All critical environment variables are present!")




