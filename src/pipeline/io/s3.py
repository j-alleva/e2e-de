"""
S3 client wrapper for cloud storage integration.

Handles uploading bronze/silver data to AWS S3 with optional idempotency checks
(check_exists parameter), path normalization, and structured logging.

Usage:
    from src.pipeline.io.s3 import S3Client
    
    s3 = S3Client()
    s3.upload_file(
        local_path="./data/bronze/source=openmeteo/run_date=2026-02-11/raw.json",
        s3_key="bronze/source=openmeteo/run_date=2026-02-11/raw.json"
    )
    
    Or with idempotency check (Block 5+):
    s3.upload_file(local_path=path, s3_key=key, check_exists=True)

Configuration:
    Requires AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_BUCKET_NAME, AWS_REGION
    (See src.pipeline.config.Project_Config and infra.md)
"""
import boto3
import logging
import os
from botocore.exceptions import ClientError
from src.pipeline.config import Project_Config
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

class S3Client:
    def __init__(self):
        """
        Initializes the S3 client using credentials from environment variables.
        """
        self.bucket_name = Project_Config.AWS_BUCKET_NAME
        self.region = Project_Config.AWS_REGION
        
        # boto3 looks for AWS keys in env
        self.client = boto3.client('s3', region_name=self.region)
        logger.info(f"S3 Client initialized for bucket: {self.bucket_name}")

    def upload_file(self, local_path: str, s3_key: str, check_exists: bool = False) -> bool:
        """
        Uploads a local file to the S3 bucket.

        Args:
            local_path: Path to the file on your local machine
            s3_key: The destination path (key) in the S3 bucket
            check_exists: If True, skip upload if object already exists in S3

        Returns:
            True if upload was successful, False otherwise.
        """

        # If no key provided, mirror local path
        # Always force forward slashes
        if s3_key is None:
            s3_key = local_path.replace(os.sep,"/")
            logger.debug(f"Auto-generated s3_key from local_path: {s3_key}")
        else:
            s3_key = s3_key.replace(os.sep,"/")
            logger.debug(f"Normalized s3_key separators: {s3_key}")

        # Remove any existing leading "./" to prevent a "." folder in S3
        if s3_key.startswith("./"):
            s3_key = s3_key[2:]
            logger.debug(f"Removed leading './': {s3_key}")

        if check_exists:
            try:
                self.client.head_object(Bucket=self.bucket_name, Key=s3_key)
                logger.info(f"Object already exists: s3://{self.bucket_name}/{s3_key} (skipping)")
                return True
            except ClientError as e:
                if e.response['Error']['Code'] != '404':
                    raise

        try:
            self.client.upload_file(local_path, self.bucket_name, s3_key)
            logger.info(f"Successfully uploaded to s3://{self.bucket_name}/{s3_key}")
            return True
        except ClientError as e:
            logger.error(f"Failed to upload {local_path} to S3: {e}")
            return False
        except FileNotFoundError:
            logger.error(f"The file was not found: {local_path}")
            return False
        