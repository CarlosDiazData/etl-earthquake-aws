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
    - **Time-Windowed Queries**: Dynamically calculates the query time window (default: past 24 hours or 365 days).
    - **Error Handling**: Robust exception handling for network issues and API rate limiting.
    - **Observability**: Structured logging for monitoring throughput and status.
    - **Efficiency**: Uses standard library `urllib` to minimize deployment package size (no external dependencies).

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
DESTINATION_FOLDER = "bronze"
DESTINATION_FILENAME = "raw_earthquakes.json"

# --- Logging Setup ---
# Configure logging to ensure visibility in CloudWatch Logs
logger = logging.getLogger()
if logger.handlers:
    # Lambda environment pre-configures a handler; we set the level.
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter('[%(levelname)s] %(asctime)s - %(message)s'))
    logger.setLevel(logging.INFO)
else:
    logging.basicConfig(level=logging.INFO, format='[%(levelname)s] %(asctime)s - %(message)s')

# --- Initialization ---
# Initialize AWS clients outside the handler to leverage execution context reuse (Warm Starts).
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

def lambda_handler(event, context):
    """
    Main Lambda entry point.

    Process Flow:
        1. Setup & Validation of Environment.
        2. Construction of USGS API Query.
        3. Execution of Data Fetching (HTTP GET).
        4. Storage of Resulting Payload to S3.

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
        destination_key = f"{DESTINATION_FOLDER}/{DESTINATION_FILENAME}"

        # 2. Query Construction
        # We fetch data for the last 365 days. 
        # Note: For production incremental loads, this should likely be 'last 24 hours' or parameterized via the event.
        end_datetime_utc = datetime.now(timezone.utc)
        start_datetime_utc = end_datetime_utc - timedelta(days=365)
        
        logger.info(f"Query Time Window: {start_datetime_utc.isoformat()} to {end_datetime_utc.isoformat()}")

        query_params = {
            'format': 'geojson',
            'starttime': start_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S'),
            'endtime': end_datetime_utc.strftime('%Y-%m-%dT%H:%M:%S'),
            'minmagnitude': 2.5, # Filter for relevant seismic activity
            'limit': 20000       # Safeguard against payload size limits
        }
        
        url_values = urllib.parse.urlencode(query_params)
        full_url = f"{USGS_API_BASE_URL}?{url_values}"
        
        # Log the query parameters (Base URL is known, params are dynamic context)
        logger.info("Constructed USGS API Query Parameters: " + str(query_params))

        # 3. Data Fetching
        logger.info("Executing API Request to USGS...")
        with urllib.request.urlopen(full_url, timeout=120) as response:
            if response.status != 200:
                raise Exception(f"External API returned non-200 status: {response.status}")
            
            earthquake_data = json.loads(response.read().decode('utf-8'))
        
        # Log metrics about the received data
        feature_count = len(earthquake_data.get('features', []))
        metadata = earthquake_data.get('metadata', {})
        logger.info(f"API Request Successful. Status: {metadata.get('status')}")
        logger.info(f"Retrieved {feature_count} earthquake records.")

        if feature_count == 0:
            logger.warning("No earthquake records found for the specified criteria.")
        
        # 4. Data Persistence (S3)
        logger.info(f"Uploading data to S3 Bucket: {s3_bucket_name}, Key: {destination_key}")
        
        if s3_client:
            s3_client.put_object(
                Bucket=s3_bucket_name,
                Key=destination_key,
                Body=json.dumps(earthquake_data),
                ContentType='application/json'
            )
        else:
            raise RuntimeError("S3 Client is not initialized.")

        elapsed_time = time.time() - start_time
        success_msg = f"Ingestion completed successfully. Records: {feature_count}. Duration: {elapsed_time:.2f}s"
        logger.info(success_msg)
        logger.info("--- Job Completed ---")

        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Data ingestion successful",
                "record_count": feature_count,
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