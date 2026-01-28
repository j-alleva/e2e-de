
"""
config file for python ingestion task

Capabilities 
1. Load environment variables through load dotenv
2. Modular class hierarchy for configuration variables
3. Robust validation to ensure presence of environment variables

"""

#import os and dotenv library, and import load_dotenv for access of environment variables
import os
from dotenv import load_dotenv

load_dotenv()

class Project_Config:

    """
    Configuration class for accessing of API and ENV variables
    Contains validation function
    """

    class Paths:
        #access data lake layer variables
        LOCAL_BRONZE = os.getenv("LOCAL_BRONZE_PATH")
        LOCAL_SILVER = os.getenv("LOCAL_SILVER_PATH")

    class API:
        #access api url variable
        OPEN_METEO = os.getenv("OPEN_METEO_URL")

    @classmethod
    def validate(cls):

        #Validates presence of critical env variables
        #Raises value error if any variables are missing
        validateCheck = [
            ("LOCAL_BRONZE_PATH",cls.Paths.LOCAL_BRONZE),
            ("LOCAL_SILVER_PATH",cls.Paths.LOCAL_SILVER),
            ("OPEN_METEO_URL",cls.API.OPEN_METEO)
        ]

        missing = []
        for name,value in validateCheck:
            if not value:
                missing.append(name)

        if missing:
            raise ValueError(
                f"Environment Variables are missing: {', '.join(missing)}. Please check your env file"
            )
        
        print("All critical environment variables are present!")

if __name__ == "__main__":

    try:
        Project_Config.validate()
        print(f"Bronze Path: {Project_Config.Paths.LOCAL_BRONZE}")
        print(f"Silver Path: {Project_Config.Paths.LOCAL_SILVER}")
        print(f"API Url: {Project_Config.API.OPEN_METEO}")

    except ValueError as e:
        print(f"Configuration Error: {e}")



