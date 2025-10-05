"""
Fetch data from URL, parse it according to project config, and upload to S3.
"""
import json
import os
import pandas as pd
import requests
import logging
from datetime import datetime

logger = logging.getLogger(__name__)


class DataFetcher:
    """Fetches and processes data according to project configuration."""

    def __init__(self, project_name, environment='dev'):
        """
        Initialize DataFetcher.

        Args:
            project_name: Name of the project (e.g., 'yf')
            environment: Environment to use (dev/qa/staging/prod)
        """
        self.project_name = project_name
        self.environment = environment

        # Validate project directory exists
        if not os.path.isdir(project_name):
            raise FileNotFoundError(f"Project directory '{project_name}' does not exist")

        # Validate config directory exists
        config_dir = f"{project_name}/config"
        if not os.path.isdir(config_dir):
            raise FileNotFoundError(f"Config directory '{config_dir}' does not exist")

        # Load project config
        config_path = f"{project_name}/config/config.json"
        if not os.path.isfile(config_path):
            raise FileNotFoundError(f"Config file '{config_path}' does not exist")

        model_config_path = f"{project_name}/config/model_config.json"
        if not os.path.isfile(model_config_path):
            raise FileNotFoundError(f"Model config file '{model_config_path}' does not exist")

        with open(config_path, 'r') as f:
            self.config = json.load(f)

        with open(model_config_path, 'r') as f:
            self.parser_config = json.load(f)

        # Initialize cloud storage based on vendor
        cloud_vendor = self.config.get('cloud_vendor', 'aws')
        if cloud_vendor == 'aws':
            from utils.cloud_storage.aws import AWSS3Storage
            region = self.config['promotion'][environment]['sagemaker']['region']
            self.storage = AWSS3Storage(region=region)
        else:
            raise ValueError(f"Unsupported cloud vendor: {cloud_vendor}")

    def fetch_raw_data(self, url):
        """
        Fetch raw data from URL.

        Args:
            url: Data source URL

        Returns:
            pandas.DataFrame: Raw data
        """
        try:
            logger.info(f"Fetching data from {url}")
            response = requests.get(url)
            response.raise_for_status()

            # Try to parse as CSV
            from io import StringIO
            df = pd.read_csv(StringIO(response.text))

            logger.info(f"Successfully fetched {len(df)} rows")
            return df

        except Exception as e:
            logger.error(f"Error fetching data: {e}")
            raise

    def parse_data(self, raw_df):
        """
        Parse raw data according to parser config.

        Args:
            raw_df: Raw pandas DataFrame

        Returns:
            pandas.DataFrame: Parsed and filtered data
        """
        try:
            selected_fields = self.parser_config.get('selected_fields', [])
            field_mappings = self.parser_config.get('field_mappings', {})

            # Apply field mappings (rename columns)
            df = raw_df.rename(columns=field_mappings)

            # Select only specified fields that exist
            available_fields = [f for f in selected_fields if f in df.columns]
            df = df[available_fields]

            logger.info(f"Parsed data with {len(df)} rows and {len(df.columns)} columns")
            return df

        except Exception as e:
            logger.error(f"Error parsing data: {e}")
            raise

    def save_to_s3(self, df, data_type='raw'):
        """
        Save DataFrame to S3.

        Args:
            df: pandas DataFrame to save
            data_type: 'raw' or 'processed'

        Returns:
            str: S3 URI
        """
        try:
            env_config = self.config['promotion'][self.environment]
            bucket = env_config['s3_bucket']

            if data_type == 'raw':
                path = env_config['s3_raw_path']
            else:
                path = env_config['s3_processed_path']

            # Generate filename with timestamp
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"data_{timestamp}.csv"

            # Save to S3
            s3_uri = self.storage.save_csv(df, bucket, path, filename)
            logger.info(f"Saved {data_type} data to {s3_uri}")

            return s3_uri

        except Exception as e:
            logger.error(f"Error saving to S3: {e}")
            raise

    def run(self, data_url):
        """
        Run complete data ingestion pipeline.

        Args:
            data_url: URL to fetch data from

        Returns:
            dict: URIs of saved raw and processed data
        """
        logger.info(f"Starting data ingestion for project '{self.project_name}' in '{self.environment}' environment")

        # Fetch raw data
        raw_df = self.fetch_raw_data(data_url)

        # Save raw data to S3
        raw_uri = self.save_to_s3(raw_df, data_type='raw')

        # Parse data
        processed_df = self.parse_data(raw_df)

        # Save processed data to S3
        processed_uri = self.save_to_s3(processed_df, data_type='processed')

        logger.info("Data ingestion completed successfully")

        return {
            'raw_uri': raw_uri,
            'processed_uri': processed_uri,
            'raw_rows': len(raw_df),
            'processed_rows': len(processed_df)
        }


def fetch_data(project_name, data_url, environment='dev'):
    """
    Convenience function to fetch and process data.

    Args:
        project_name: Name of the project
        data_url: URL to fetch data from
        environment: Environment (dev/qa/staging/prod)

    Returns:
        dict: Results of data ingestion
    """
    fetcher = DataFetcher(project_name, environment)
    return fetcher.run(data_url)
