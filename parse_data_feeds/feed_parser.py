#!/usr/bin/env python3
"""
Script to pull data from URL and upload to S3 based on project configuration.

Usage:
    python parse_data_feeds/feed_parser.py --project yf --env dev
    python parse_data_feeds/feed_parser.py --project yf --url <data_url> --env qa
    PROJECT=yf python parse_data_feeds/feed_parser.py --env prod
"""
import argparse
import sys
import os
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from utils.fetch_data import fetch_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def main():
    """Main entry point for data pulling script."""
    parser = argparse.ArgumentParser(
        description='Pull data from URL and upload to S3 based on project config'
    )
    parser.add_argument(
        '--project',
        default=None,
        help='Project name (e.g., yf). If not provided, uses PROJECT env variable'
    )
    parser.add_argument(
        '--url',
        default=None,
        help='URL to fetch data from. If not provided, reads from model_config.json'
    )
    parser.add_argument(
        '--env',
        default='dev',
        choices=['dev', 'qa', 'staging', 'prod'],
        help='Environment (default: dev)'
    )

    args = parser.parse_args()

    # Get project name from args or environment variable
    project = args.project or os.environ.get('PROJECT')
    if not project:
        logger.error("Project name is required. Provide --project or set PROJECT environment variable")
        sys.exit(1)

    try:
        logger.info(f"Starting data pull for project: {project}")
        logger.info(f"Environment: {args.env}")

        # If URL not provided, read from model_config.json
        data_url = args.url
        if not data_url:
            model_config_path = f"{project}/config/model_config.json"
            if not os.path.isfile(model_config_path):
                logger.error(f"URL not provided and model_config.json not found at {model_config_path}")
                sys.exit(1)

            import json
            with open(model_config_path, 'r') as f:
                model_config = json.load(f)

            data_url = model_config.get('data_url')
            if not data_url:
                logger.error("URL not provided and 'data_url' not found in model_config.json")
                sys.exit(1)

            logger.info(f"Using data_url from model_config.json")

        logger.info(f"Data URL: {data_url}")

        # Fetch and process data
        result = fetch_data(
            project_name=project,
            data_url=data_url,
            environment=args.env
        )

        # Print results
        logger.info("=" * 60)
        logger.info("Data Ingestion Results:")
        logger.info(f"  Raw Data URI: {result['raw_uri']}")
        logger.info(f"  Processed Data URI: {result['processed_uri']}")
        logger.info(f"  Raw Rows: {result['raw_rows']}")
        logger.info(f"  Processed Rows: {result['processed_rows']}")
        logger.info("=" * 60)

        print("\nData ingestion completed successfully!")
        print(f"Raw data saved to: {result['raw_uri']}")
        print(f"Processed data saved to: {result['processed_uri']}")

    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        logger.error(f"Make sure {project}/config/config.json and {project}/config/model_config.json exist")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error during data ingestion: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
