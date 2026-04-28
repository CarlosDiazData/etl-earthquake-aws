"""
USGS Earthquake Data Ingestion Lambda
=====================================

Description:
    This serverless function acts as the entry point for the Earthquake ETL pipeline.
    It is designed to be triggered on a schedule (e.g., via Amazon EventBridge) to
    fetch recent earthquake data from the USGS (United States Geological Survey)
    REST API and ingest it into the raw data storage layer (Bronze Layer).

Architecture:
    [USGS API] -> [Lambda Ingestion] -> [Amazon S3 (Bronze/Raw)]

Key Features:
    - **Initial Backfill**: First execution fetches up to 20,000 records (API limit)
      from the past year to populate the data lake.
    - **Incremental Loads**: Subsequent executions fetch only the last 48 hours,
      reducing cost and processing time.
    - **Partitioned Storage**: Data stored as bronze/{year}/{month}/{day}/{hour}/
      for efficient downstream processing.
    - **Initial Load Tracking**: Uses a marker file in S3 to track backfill completion.
    - **Observability**: Structured logging for monitoring throughput and status.
    - **Efficiency**: Uses standard library `urllib` to minimize deployment package size.

Environment Variables:
    - `S3_BUCKET_NAME`: (Required) The target S3 bucket for storing raw JSON data.
"""

import json
import logging
import os
import boto3
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime, timedelta, timezone
import time

# --- Configuration & Constants ---
USGS_API_BASE_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
BRONZE_PREFIX = "bronze"
MARKER_KEY = ".initial_load_complete"
API_LIMIT = 20000  # USGS API maximum records per request

# --- Logging Setup ---
logger = logging.getLogger()
if logger.handlers:
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s'))
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')

# --- Initialization ---
try:
    s3_client = boto3.client('s3')
except Exception as e:
    logger.critical(f"Failed to initialize global S3 client. Error: {e}", exc_info=True)
    s3_client = None


def get_config_or_raise():
    """
    Retrieves and validates necessary environment variables.
    Raises:
        ValueError: If S3_BUCKET_NAME is missing.
    """
    bucket_name = os.environ.get("S3_BUCKET_NAME")
    if not bucket_name:
        raise ValueError("Environment variable 'S3_BUCKET_NAME' is not defined.")
    return bucket_name


def check_initial_load_complete(bucket_name: str) -> bool:
    """
    Checks if the initial load marker file exists in S3.
    Returns True if initial backfill is complete, False otherwise.
    """
    try:
        s3_client.head_object(Bucket=bucket_name, Key=MARKER_KEY)
        logger.info("Initial load marker found. Running incremental ingestion.")
        return True
    except s3_client.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == '404':
            logger.info("No initial load marker found. Running initial backfill.")
            return False
        # For other errors (AccessDenied, etc.), log and assume incremental
        logger.warning(f"Error checking initial load marker: {e}. Assuming incremental.")
        return True


def set_initial_load_complete(bucket_name: str):
    """
    Creates the initial load marker file in S3 after backfill completes.
    """
    try:
        s3_client.put_object(
            Bucket=bucket_name,
            Key=MARKER_KEY,
            Body=json.dumps({
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "message": "Initial backfill complete. Switching to incremental mode."
            }),
            ContentType='application/json'
        )
        logger.info("Initial load marker created. Future runs will use incremental mode.")
    except Exception as e:
        logger.error(f"Failed to create initial load marker: {e}")


def build_partitioned_key(end_datetime: datetime) -> str:
    """
    Builds a time-partitioned S3 key: bronze/{year}/{month}/{day}/{hour}/raw_earthquakes.json
    """
    return (
        f"{BRONZE_PREFIX}/"
        f"{end_datetime.year}/"
        f"{end_datetime.month:02d}/"
        f"{end_datetime.day:02d}/"
        f"{end_datetime.hour:02d}/"
        f"raw_earthquakes.json"
    )


def fetch_with_retry(url: str, max_retries: int = 3, timeout: int = 120) -> dict:
    """
    Fetches data from URL with retry logic for transient failures.
    """
    last_error = None
    for attempt in range(max_retries):
        try:
            with urllib.request.urlopen(url, timeout=timeout) as response:
                if response.status != 200:
                    raise Exception(f"External API returned non-200 status: {response.status}")
                return json.loads(response.read().decode('utf-8'))
        except urllib.error.HTTPError as e:
            last_error = e
            logger.warning(f"HTTP Error {e.code} on attempt {attempt + 1}. Retrying...")
        except urllib.error.URLError as e:
            last_error = e
            logger.warning(f"URL Error on attempt {attempt + 1}. Retrying...")
        except Exception as e:
            last_error = e
            logger.warning(f"Unexpected error on attempt {attempt + 1}. Retrying...")

        if attempt < max_retries - 1:
            time.sleep(2 ** attempt)  # Exponential backoff

    raise Exception(f"Failed to fetch data after {max_retries} attempts. Last error: {last_error}")


def lambda_handler(event, context):
    """
    Main Lambda entry point.

    Process Flow:
        1. Setup & Validation of Environment.
        2. Check if initial load is complete (via S3 marker).
        3. Construct USGS API Query (full backfill vs incremental).
        4. Execute Data Fetching (HTTP GET with retry).
        5. Store partitioned data to S3.
        6. If initial load, create marker file.

    Args:
        event (dict): Lambda event payload (e.g., from EventBridge).
        context (object): Lambda context object providing runtime info.

    Returns:
        dict: API Gateway style response object (statusCode, body).
    """
    start_time = time.time()
    logger.info("--- Starting Earthquake Data Ingestion Job ---")
    logger.info(f"Execution Request ID: {context.aws_request_id}")

    try:
        # 1. Configuration Validation
        s3_bucket_name = get_config_or_raise()

        # 2. Determine ingestion mode
        is_incremental = check_initial_load_complete(s3_bucket_name)

        # 3. Query Construction
        end_datetime_utc = datetime.now(timezone.utc)

        if is_incremental:
            # Incremental mode: fetch last 48 hours
            start_datetime_utc = end_datetime_utc - timedelta(hours=48)
            ingestion_mode = "INCREMENTAL"
        else:
            # Initial backfill mode: fetch up to API limit from past year
            # USGS API has a 20,000 record limit, so we request 365 days
            # and let the API return what it can (up to 20k)
            start_datetime_utc = end_datetime_utc - timedelta(days=365)
            ingestion_mode = "INITIAL_BACKFILL"

        logger.info(f"Ingestion Mode: {ingestion_mode}")
        logger.info(f"Query Time Window: {start_datetime_utc.isoformat()} to {end_datetime_utc.isoformat()}")

        query_params = {
            'format': 'geojson',
            'starttime': start_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S'),
            'endtime': end_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S'),
            'minmagnitude': 2.5,
            'limit': API_LIMIT
        }

        url_values = urllib.parse.urlencode(query_params)
        full_url = f"{USGS_API_BASE_URL}?{url_values}"

        logger.info("Constructed USGS API Query Parameters: " + str(query_params))

        # 4. Data Fetching with retry
        logger.info("Executing API Request to USGS...")
        earthquake_data = fetch_with_retry(full_url)

        # Log metrics about the received data
        feature_count = len(earthquake_data.get('features', []))
        metadata = earthquake_data.get('metadata', {})
        logger.info(f"API Request Successful. Status: {metadata.get('status')}")
        logger.info(f"Retrieved {feature_count} earthquake records.")

        if feature_count == 0:
            logger.warning("No earthquake records found for the specified criteria.")
            return {
                "statusCode": 200,
                "body": json.dumps({
                    "message": "No new earthquake data found",
                    "record_count": 0,
                    "ingestion_mode": ingestion_mode
                })
            }

        # 5. Data Persistence - Partitioned by time
        # Use the end_datetime for partitioning to represent the "batch" time
        destination_key = build_partitioned_key(end_datetime_utc)
        logger.info(f"Uploading data to S3 Bucket: {s3_bucket_name}, Key: {destination_key}")

        s3_client.put_object(
            Bucket=s3_bucket_name,
            Key=destination_key,
            Body=json.dumps(earthquake_data),
            ContentType='application/json'
        )

        # 6. Mark initial load as complete after first successful backfill
        if not is_incremental and feature_count > 0:
            set_initial_load_complete(s3_bucket_name)
            logger.info("Initial backfill completed successfully.")

        elapsed_time = time.time() - start_time
        success_msg = (
            f"Ingestion completed successfully. "
            f"Mode: {ingestion_mode}, Records: {feature_count}, Duration: {elapsed_time:.2f}s"
        )
        logger.info(success_msg)
        logger.info("--- Job Completed ---")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data ingestion successful",
                "record_count": feature_count,
                "ingestion_mode": ingestion_mode,
                "s3_location": f"s3://{s3_bucket_name}/{destination_key}"
            })
        }

    except urllib.error.URLError as e:
        logger.error(f"Network/API Error connecting to USGS: {e}", exc_info=True)
        return {"statusCode": 502, "body": f"External API Connection Failed: {e}"}

    except ValueError as e:
        logger.error(f"Configuration Error: {e}")
        return {"statusCode": 500, "body": f"Configuration Error: {e}"}

    except Exception as e:
        logger.critical(f"Unhandled Exception in Lambda Handler: {e}", exc_info=True)
        return {"statusCode": 500, "body": f"Internal Server Error: {str(e)}"}