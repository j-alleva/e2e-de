
"""

Configuration File for data ingestion pipeline 

Manages environment variables, generates partitioned data lake paths
(bronze/silver layers), and constructs API URLs for weather data sources.
Validates required configuration on pipeline startup

Classes:
    Project_Config: Main configuration container with nested Paths and API classes

Environment Variables Required:
    LOCAL_BRONZE_PATH: Base path for raw data storage
    LOCAL_SILVER_PATH: Base path for cleaned data storage
    LOCAL_GOLD_PATH: Base path for curated data storage (Placeholder)
    OPEN_METEO_URL_TEMPLATE: URL template for Open-Meteo API

Environment Variables Optional (Block 2+):
    POSTGRES_USER: PostgreSQL username (default: admin)
    POSTGRES_PASSWORD: PostgreSQL password (default: password)
    POSTGRES_HOST: PostgreSQL host (default: localhost)
    POSTGRES_PORT: PostgreSQL port (default: 5432)
    POSTGRES_DB: PostgreSQL database name (default: warehouse) 
"""
from typing import Optional
import os
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

class Project_Config:

    """
    
    Main configuration container for the ingestion pipeline.

    Provides environment variable access, path generation for data lake layers,
    and API URL construction.

    Attributes:
        LOCATION_LOOKUP: Mapping of location names to coordinates

    """
    LOCATION_LOOKUP = {
        "Boston" : {"latitude": 42.3601, "longitude": -71.0589}
    }
    AWS_BUCKET_NAME = os.getenv("AWS_BUCKET_NAME")
    AWS_REGION = os.getenv("AWS_REGION")

    class Paths:
        """
        Data lake path generation for bronze and silver layers.
        """
        LOCAL_BRONZE = os.getenv("LOCAL_BRONZE_PATH")
        LOCAL_SILVER = os.getenv("LOCAL_SILVER_PATH")
        LOCAL_GOLD = os.getenv("LOCAL_GOLD_PATH")
        
        @classmethod
        def bronze_path(cls, source: str, run_date: str, location: Optional[str]=None) -> str:
            """
            Generate partitioned bronze layer path for raw data storage.

            Args:
                source: Data source identifier (e.g., 'openmeteo')
                run_date: Date in YYYY-MM-DD format
                location: Optional location name for additional partitioning

            Returns:
                Formatted path string like 'data/bronze/source=X/run_date=Y/location=Z'

            """
            path = f"{cls.LOCAL_BRONZE}/source={source}/run_date={run_date}"
            if location:
                path += f"/location={location}"
            logger.debug(f"Generated bronze path: {path}")
            return path
        
        @classmethod
        def silver_path(cls, source: str, run_date: str, location: Optional[str]=None) -> str:
            """
            Generate partitioned silver layer path for cleaned data storage.

            Args:
                source: Data source identifier (e.g., 'openmeteo')
                run_date: Date in YYYY-MM-DD format
                location: Optional location name for additional partitioning

            Returns:
                Formatted path string like 'data/silver/source=X/run_date=Y/location=Z'
                
            """
            path = f"{cls.LOCAL_SILVER}/source={source}/run_date={run_date}"
            if location:
                path += f"/location={location}"
            logger.debug(f"Generated silver path: {path}")
            return path
        
        @classmethod
        def gold_path(cls, source: str, run_date: str, location: Optional[str]=None) -> str:
            """
            Generate partitioned gold layer path for cleaned data storage. (Placeholder)

            Args:
                source: Data source identifier (e.g., 'openmeteo')
                run_date: Date in YYYY-MM-DD format
                location: Optional location name for additional partitioning

            Returns:
                Formatted path string like 'data/gold/source=X/run_date=Y/location=Z'
            """
            path = f"{cls.LOCAL_GOLD}/source={source}/run_date={run_date}"
            if location:
                path += f"/location={location}"
            logger.debug(f"Generated gold path: {path}")
            return path
        
    class API:
        """
        API URL construction for external data sources.
        """
        
        OPEN_METEO_URL_TEMPLATE = os.getenv("OPEN_METEO_URL_TEMPLATE")

        @classmethod
        def get_open_meteo_url(cls,location : str) -> str:
            """
            Construct Open-Meteo API URL with location coordinates.

            Args:
                location: Location name from LOCATION_LOOKUP dictionary

            Returns:
                Complete API URL with latitude and longitude parameters
            """
            coords = Project_Config.LOCATION_LOOKUP[location]
            if cls.OPEN_METEO_URL_TEMPLATE is None:
                raise ValueError("OPEN_METEO_URL_TEMPLATE environment variable is not set")
            url = cls.OPEN_METEO_URL_TEMPLATE.format(lat=coords["latitude"],lon=coords["longitude"])
            logger.debug(f"Generated API URL for {location}: {url}")
            return url
        
    class Database:
        """
        PostgreSQL database connection configuration.
        """
        POSTGRES_USER = os.getenv("POSTGRES_USER", "admin")
        POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "password")
        POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
        POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
        POSTGRES_DB = os.getenv("POSTGRES_DB", "warehouse")

        @classmethod
        def connection_string(cls) -> str:
            """
            Build SQLAlchemy PostgreSQL connection string.

            Returns:
                Connection string in format: postgresql://user:pass@host:port/database
            """
            conn_str = f"postgresql://{cls.POSTGRES_USER}:{cls.POSTGRES_PASSWORD}@{cls.POSTGRES_HOST}:{cls.POSTGRES_PORT}/{cls.POSTGRES_DB}"
            logger.debug("PostgreSQL connection string constructed (password masked)")
            return conn_str
        
    @classmethod
    def validate(cls) -> None:
        """
        Validate presence of required environment variables.

        Raises:
            ValueError: If any required environment variables are missing
        """

        validateCheck = [
            ("LOCAL_BRONZE_PATH",cls.Paths.LOCAL_BRONZE),
            ("LOCAL_SILVER_PATH",cls.Paths.LOCAL_SILVER),
            ("OPEN_METEO_URL_TEMPLATE",cls.API.OPEN_METEO_URL_TEMPLATE),
            ("AWS_BUCKET_NAME", cls.AWS_BUCKET_NAME),
            ("AWS_REGION",cls.AWS_REGION)
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




