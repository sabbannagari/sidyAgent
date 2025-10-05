"""
AWS S3 cloud storage utilities for saving and loading CSV files.
"""
import boto3
import pandas as pd
from io import StringIO
import logging

logger = logging.getLogger(__name__)


class AWSS3Storage:
    """AWS S3 storage handler for CSV operations."""

    def __init__(self, region='us-east-1'):
        """
        Initialize AWS S3 client.

        Args:
            region: AWS region (default: us-east-1)
        """
        self.s3_client = boto3.client('s3', region_name=region)
        self.region = region

    def save_csv(self, dataframe, bucket, path, filename):
        """
        Save a pandas DataFrame to S3 as CSV.

        Args:
            dataframe: pandas DataFrame to save
            bucket: S3 bucket name
            path: S3 path (prefix)
            filename: CSV filename

        Returns:
            str: S3 URI of saved file
        """
        try:
            # Construct full S3 key
            s3_key = f"{path}{filename}"

            # Convert DataFrame to CSV
            csv_buffer = StringIO()
            dataframe.to_csv(csv_buffer, index=False)

            # Upload to S3
            self.s3_client.put_object(
                Bucket=bucket,
                Key=s3_key,
                Body=csv_buffer.getvalue()
            )

            s3_uri = f"s3://{bucket}/{s3_key}"
            logger.info(f"Successfully saved CSV to {s3_uri}")
            return s3_uri

        except Exception as e:
            logger.error(f"Error saving CSV to S3: {e}")
            raise

    def load_csv(self, bucket, path, filename):
        """
        Load a CSV file from S3 into a pandas DataFrame.

        Args:
            bucket: S3 bucket name
            path: S3 path (prefix)
            filename: CSV filename

        Returns:
            pandas.DataFrame: Loaded data
        """
        try:
            # Construct full S3 key
            s3_key = f"{path}{filename}"

            # Get object from S3
            response = self.s3_client.get_object(Bucket=bucket, Key=s3_key)

            # Read CSV into DataFrame
            df = pd.read_csv(response['Body'])

            logger.info(f"Successfully loaded CSV from s3://{bucket}/{s3_key}")
            return df

        except Exception as e:
            logger.error(f"Error loading CSV from S3: {e}")
            raise

    def list_files(self, bucket, prefix):
        """
        List all files in S3 bucket with given prefix.

        Args:
            bucket: S3 bucket name
            prefix: S3 prefix/path

        Returns:
            list: List of S3 keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=prefix
            )

            if 'Contents' in response:
                files = [obj['Key'] for obj in response['Contents']]
                logger.info(f"Found {len(files)} files in s3://{bucket}/{prefix}")
                return files
            else:
                logger.info(f"No files found in s3://{bucket}/{prefix}")
                return []

        except Exception as e:
            logger.error(f"Error listing files from S3: {e}")
            raise
