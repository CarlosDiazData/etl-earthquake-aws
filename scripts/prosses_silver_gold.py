import sys
import logging
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta, date

"""
Process Silver to Gold Script (Dimensional Modeling)
=====================================================

Description:
    This Glue job transforms the cleaned earthquake data from the Silver layer (Delta Lake) 
    into a Gold level Star Schema optimized for analytical queries (OLAP).

    The resulting schema consists of:
    - **Fact Table**: `fact_earthquake_events` containing measures and foreign keys.
    - **Dimension Tables**:
        - `dim_date`: Comprehensive date attributes.
        - `dim_location`: Unique geographical locations (lat/lon, country, region).
        - `dim_magnitude`: Categorical grouping of earthquake magnitudes.
        - `dim_event_type`: Types of events and magnitude measurement methods.

Key Steps:
    1.  **Ingestion**: Read the optimized Delta table from the Silver layer.
    2.  **Dimension Creation**:
        - Generate dimensions using distinct values and lookups.
        - Assign surrogate keys (Monotonically Increasing IDs) to dimensions.
    3.  **Fact Table Construction**:
        - Join the source data with the new dimensions to retrieve Surrogate Keys.
        - Select relevant measures (magnitude, depth, etc.).
    4.  **Load**: Write the resulting tables to the Gold layer (S3) in Parquet format 
        and register them in the Glue Data Catalog (Athena).

Usage:
    Executed via AWS Glue. Requires 'S3_BUCKET_NAME' as a job parameter.
"""

# --- 1. Glue and Spark Configuration ---
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET_NAME'])

# Standard Glue Initialization
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize Python standard logger ( avoids Py4J errors on Spark executors )
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
_handler = logging.StreamHandler()
_handler.setFormatter(logging.Formatter("%(levelname)s: %(message)s"))
logger.addHandler(_handler)
logger.propagate = False
logger.info("Initializing Silver to Gold ETL Job (Dimensional Modeling)...")

# Configuration & Paths
S3_BUCKET = args['S3_BUCKET_NAME']
# Input: Silver Layer (Delta)
SILVER_PATH = f"s3://{S3_BUCKET}/silver/earthquakes_cleaned/"
# Output: Gold Layer (Parquet)
GOLD_PATH_BASE = f"s3://{S3_BUCKET}/gold/"
# Target Database in Glue Catalog / Athena
GOLD_DATABASE = "gold_earthquakes"

def main():
    """
    Main entry point for the Silver to Gold transformation.
    """
    logger.info(f"Configuration - S3 Bucket: {S3_BUCKET}")
    logger.info(f"Source Path: {SILVER_PATH}")
    logger.info(f"Target Database: {GOLD_DATABASE}")

    # --- 2. Ingestion Phase ---
    logger.info("--- Step 2: Reading Silver Layer Data ---")
    try:
        logger.info(f"Reading Delta table from: {SILVER_PATH}")
        silver_exists = spark._jvm().org.apache.hadoop.fs.FileSystem \
            .get(spark._jsparkSession.sparkContext().hadoopConfiguration()) \
            .exists(spark._jvm().org.apache.hadoop.fs.Path(SILVER_PATH))

        if not silver_exists:
            logger.warning(f"Silver path does not exist: {SILVER_PATH}. Skipping gold processing.")
            return

        df_silver = spark.read.format("delta").load(SILVER_PATH)

        if df_silver.rdd.isEmpty():
            logger.warning("Silver layer is empty. No data to process. Job finished successfully (no-op).")
            return
            
        record_count = df_silver.count()
        logger.info(f"Successfully read {record_count} records from the Silver layer.")
    except Exception as e:
        logger.error(f"Failed to read from Silver layer. Error: {e}")
        raise

    # Ensure the Gold database exists
    logger.info(f"Ensuring database '{GOLD_DATABASE}' exists...")
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DATABASE}")

    # --- 3. Dimension Modeling Phase ---
    logger.info("--- Step 3: Creating Dimension Tables ---")

    # 3.1 DimDate: A comprehensive calendar dimension
    # Used for analyzing trends over time (Yearly, Monthly, Weekly).
    logger.info("Generating 'DimDate' (Calendar Dimension)...")
    
    # Determine the date range based on the data
    min_max_date = df_silver.select(
        F.min("event_timestamp_utc").alias("min_date"),
        F.max("event_timestamp_utc").alias("max_date")
    ).first()
    
    start_date = min_max_date["min_date"].date()
    end_date = min_max_date["max_date"].date() + timedelta(days=30) # Add a 30-day buffer
    
    logger.info(f"Date Range: {start_date} to {end_date}")

    date_list = []
    current_date = start_date
    while current_date <= end_date:
        date_list.append({
            'DateKey': int(current_date.strftime('%Y%m%d')), # Integer PK (YYYYMMDD)
            'FullDate': current_date,
            'Year': current_date.year,
            'Quarter': (current_date.month - 1) // 3 + 1,
            'Month': current_date.month,
            'MonthName': current_date.strftime('%B'),
            'DayOfMonth': current_date.day,
            'DayOfWeek': current_date.isoweekday() % 7 + 1,
            'DayName': current_date.strftime('%A'),
            'IsWeekend': 1 if current_date.weekday() >= 5 else 0,
        })
        current_date += timedelta(days=1)
        
    df_dim_date = spark.createDataFrame(date_list)
    logger.info(f"DimDate created with {df_dim_date.count()} days.")

    # 3.2 DimLocation: Geographical context
    # Captures unique locations to reduce redundancy in the fact table.
    # Uses deterministic hash-based key: same location always gets same key.
    logger.info("Generating 'DimLocation'...")
    df_dim_location = df_silver.select(
        "latitude", "longitude", "place", "extracted_country",
        "extracted_region_detail", "hemisphere_ns", "hemisphere_ew"
    ).distinct() \
    .withColumn("LocationKey",
        F.substring(
            F.md5(concat(
                F.col("latitude").cast("string"),
                F.lit("|"),
                F.col("longitude").cast("string"),
                F.lit("|"),
                F.coalesce(F.col("place"), F.lit("NULL"))
            )), 0, 16
        ).cast("bigint")
    )
    logger.info(f"DimLocation created with {df_dim_location.count()} unique locations.")

    # 3.3 DimMagnitude: Static Reference Dimension
    # Maps continuous magnitude values to human-readable categories/descriptions.
    logger.info("Generating 'DimMagnitude'...")
    magnitude_data = [
        {"MagnitudeCategory": "Micro", "MinMagnitude": -2.0, "MaxMagnitude": 2.9, "Description": "Not felt or rarely felt."},
        {"MagnitudeCategory": "Minor", "MinMagnitude": 3.0, "MaxMagnitude": 3.9, "Description": "Often felt, rarely causes damage."},
        {"MagnitudeCategory": "Light", "MinMagnitude": 4.0, "MaxMagnitude": 4.9, "Description": "Felt by many, possible minor damage."},
        {"MagnitudeCategory": "Moderate", "MinMagnitude": 5.0, "MaxMagnitude": 5.9, "Description": "Damage to weak structures."},
        {"MagnitudeCategory": "Strong", "MinMagnitude": 6.0, "MaxMagnitude": 6.9, "Description": "Moderate damage to well-built structures."},
        {"MagnitudeCategory": "Major", "MinMagnitude": 7.0, "MaxMagnitude": 7.9, "Description": "Serious damage to most buildings."},
        {"MagnitudeCategory": "Great", "MinMagnitude": 8.0, "MaxMagnitude": 10.0, "Description": "Widespread destruction."},
        {"MagnitudeCategory": "Unknown", "MinMagnitude": None, "MaxMagnitude": None, "Description": "Category not determined."}
    ]

    df_dim_magnitude = spark.createDataFrame(magnitude_data) \
        .withColumn("MagnitudeKey", F.monotonically_increasing_id())

    # 3.4 DimEventType: Event Classifications
    # Uses deterministic hash-based key: same (event_type, magType) always gets same key.
    logger.info("Generating 'DimEventType'...")
    df_dim_event_type = df_silver.select("event_type", "magType").distinct() \
    .withColumn("EventTypeKey",
        F.substring(
            F.md5(concat(
                F.coalesce(F.col("event_type"), F.lit("NULL")),
                F.lit("|"),
                F.coalesce(F.col("magType"), F.lit("NULL"))
            )), 0, 16
        ).cast("bigint")
    )

    # --- 4. Fact Table Construction Phase ---
    logger.info("--- Step 4: Constructing FactEarthquakeEvents ---")
    
    # 4.1 Prepare Source for Joining
    # Generate the DateKey in the source data to match DimDate
    df_fact_source = df_silver.withColumn("DateKey", F.date_format(F.col("event_timestamp_utc"), "yyyyMMdd").cast("int"))

    # 4.2 Join Source with Dimensions (Surrogate Key Lookup)
    # Ideally, left joins + handling nulls is safer, but inner joins are used here 
    # assuming reference integrity from the source generation above.
    logger.info("Joining source data with dimensions...")
    df_fact_joined = df_fact_source \
        .join(df_dim_date.select("DateKey"), "DateKey", "inner") \
        .join(df_dim_location, ["latitude", "longitude", "place"], "inner") \
        .join(df_dim_magnitude, df_fact_source.magnitude_category == df_dim_magnitude.MagnitudeCategory, "inner") \
        .join(df_dim_event_type, ["event_type", "magType"], "inner")

    # 4.3 Final Column Selection
    df_fact_final = df_fact_joined.select(
        F.col("event_id").alias("EventID"),
        F.col("DateKey"),
        F.col("LocationKey"),
        F.col("MagnitudeKey"),
        F.col("EventTypeKey"),
        F.col("magnitude").alias("Magnitude"),
        F.col("depth_km").alias("DepthKm"),
        F.col("tsunami_warning").alias("TsunamiWarning"),
        F.col("significance").alias("Significance"),
        F.col("felt_reports").alias("FeltReports"),
        F.col("nst_stations").alias("NumberOfStations"),
        F.col("rms_travel_time").alias("RmsTravelTime"),
        F.col("gap_azimuthal").alias("AzimuthalGap"),
        F.col("url").alias("SourceURL"),
        F.col("silver_processing_timestamp_utc").alias("SilverProcessingTimestampUTC"),
        F.current_timestamp().alias("DWLoadTimestampUTC")
    ).dropDuplicates(["EventID"])
    
    logger.info(f"Fact Table constructed. Record count: {df_fact_final.count()}")

    # --- 5. Data Loading Phase ---
    logger.info("--- Step 5: Writing and Registering Tables (Glue Catalog) ---")
    
    def write_to_gold(df, table_name):
        """
        Helper function to write a DataFrame to S3 (Parquet) and register it in the Hive Metastore.
        """
        full_table_name = f"{GOLD_DATABASE}.{table_name}"
        s3_output_path = f"{GOLD_PATH_BASE}{table_name}/"
        
        logger.info(f"Processing Table: {full_table_name}")
        logger.info(f"Target S3 Path: {s3_output_path}")
        
        try:
            # saveAsTable manages both the data write and the catalog metadata update
            df.write \
              .format("parquet") \
              .mode("overwrite") \
              .option("path", s3_output_path) \
              .saveAsTable(full_table_name)
              
            logger.info(f"Successfully created table: {table_name}")
        except Exception as e:
            logger.error(f"Failed to write table {table_name}. Error: {e}")
            raise

    try:
        # Write Dimensions
        write_to_gold(df_dim_date, "dim_date")
        write_to_gold(df_dim_location, "dim_location")
        write_to_gold(df_dim_magnitude, "dim_magnitude")
        write_to_gold(df_dim_event_type, "dim_event_type")
        
        # Write Fact
        write_to_gold(df_fact_final, "fact_earthquake_events")
        
        logger.info("--- Silver to Gold (Dimensional Model) Job COMPLETED SUCCESSFULLY ---")
    except Exception as e:
        logger.error(f"Critical error during Gold layer writing: {e}")
        raise e

if __name__ == "__main__":
    main()
    job.commit()