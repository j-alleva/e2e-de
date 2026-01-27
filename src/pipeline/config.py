"""
config.py
---------
Centralized configuration management for the Data Engineering pipeline.

Capabilities:
1. Loads environment variables using python-dotenv.
2. Provides structured access to file paths and API settings.
3. Validates that critical configuration is present before execution.
"""

import os
from dotenv import load_dotenv

# Load environment variables from the .env file immediately
load_dotenv()

class ProjectConfig:
    """
    Main configuration object.
    Access via: ProjectConfig.Paths.LOCAL_BRONZE or ProjectConfig.API.OPEN_METEO_URL
    """
    
    class Paths:
        """Local file system paths for the data lake layers."""
        # Using .get() allows us to handle missing keys gracefully in the validator
        LOCAL_BRONZE = os.getenv("LOCAL_BRONZE_PATH")
        LOCAL_SILVER = os.getenv("LOCAL_SILVER_PATH")
        # TODO: Add GOLD path in Week X

    class API:
        """External API configuration endpoints."""
        OPEN_METEO_URL = os.getenv("OPEN_METEO_URL")

    @classmethod
    def validate(cls):
        """
        Checks for the existence of required environment variables.
        Raises:
            ValueError: If any required variable is missing.
        """
        required_vars = [
            ("LOCAL_BRONZE_PATH", cls.Paths.LOCAL_BRONZE),
            ("LOCAL_SILVER_PATH", cls.Paths.LOCAL_SILVER),
            ("OPEN_METEO_URL", cls.API.OPEN_METEO_URL)
        ]

        missing = [name for name, value in required_vars if not value]

        if missing:
            raise ValueError(
                f"Missing critical environment variables: {', '.join(missing)}. "
                "Please verify your .env file."
            )
        
        print("✅ Configuration validated successfully.")

# --- Execution Check ---
# This allows you to run `python src/pipeline/config.py` to verify setup instantly.
if __name__ == "__main__":
    try:
        ProjectConfig.validate()
        print(f"Bronze Path: {ProjectConfig.Paths.LOCAL_BRONZE}")
        print(f"API URL: {ProjectConfig.API.OPEN_METEO_URL}")
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")