import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import TimestampType, IntegerType, DoubleType, BooleanType

"""
Process Bronze to Silver Script
================================

Description:
    This Glue job is responsible for reading raw earthquake data from the Bronze layer (S3),
    flattening the nested JSON structure, cleaning and validating the data, performing 
    deduplication, and enriching the dataset with derived features before writing it 
    to the Silver layer (Delta Lake).

Key Steps:
    1.  **Configuration**: Initialize Spark/Glue contexts and retrieve job arguments.
    2.  **Ingestion**: Read raw JSON files from the Bronze S3 path.
    3.  **Flattening**: Explode and extract fields from the GeoJSON `features` structure.
    4.  **Transformation**:
        - Cast columns to appropriate data types.
        - Validate data ranges (e.g., latitude/longitude limits, positive depth).
        - Deduplicate events based on `event_id`, keeping the latest update.
    5.  **Enrichment**: Add categorical buckets for magnitude/depth and extract location details.
    6.  **Load**: Write the processed data to the Silver layer in Delta format, partitioned by year/month.

Usage:
    Executed via AWS Glue. Requires 'S3_BUCKET_NAME' as a job parameter.
"""

# --- 1. Glue and Spark Configuration ---

# Retrieve arguments passed to the Glue Job
# 'S3_BUCKET_NAME' is a custom parameter defining the root bucket for data layers.
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_BUCKET_NAME'])

# Standard Glue Initialization
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize Logger
logger = glueContext.get_logger()
logger.info("Initializing Bronze to Silver ETL Job...")

# Environment Variables & Paths
S3_BUCKET = args['S3_BUCKET_NAME']
# Input: Raw JSON data
BRONZE_PATH = f"s3://{S3_BUCKET}/bronze/" 
# Output: Cleaned Delta Table
SILVER_PATH = f"s3://{S3_BUCKET}/silver/earthquakes_cleaned/"

def main():
    """
    Main entry point for the ETL processing logic.
    """
    logger.info(f"Configuration - S3 Bucket: {S3_BUCKET}")
    logger.info(f"Source Path: {BRONZE_PATH}")
    logger.info(f"Target Path: {SILVER_PATH}")

    # --- 2. Ingestion & Flattening Phase ---
    logger.info("--- Step 2: Ingestion & Flattening ---")
    
    try:
        logger.info(f"Reading raw JSON data from: {BRONZE_PATH}")
        df_bronze_raw = spark.read.json(BRONZE_PATH)

        # Graceful exit if no data found
        if df_bronze_raw.rdd.isEmpty():
            logger.warning("Bronze layer is empty. No data to process. Job finished successfully (no-op).")
            return

        # Flattening Strategy:
        # The USGS API returns a FeatureCollection. We explode the 'features' array
        # to get one row per earthquake event.
        df_features = df_bronze_raw.select(F.explode("features").alias("feature"))

        # Extract specific properties from the hierarchical JSON structure
        df_bronze = df_features.select(
            F.col("feature.id").alias("id"),
            F.col("feature.properties.mag").alias("mag"),
            F.col("feature.properties.place").alias("place"),
            F.col("feature.properties.time").alias("time"),
            F.col("feature.properties.updated").alias("updated"),
            F.col("feature.properties.url").alias("url"),
            F.col("feature.properties.felt").alias("felt"),
            F.col("feature.properties.cdi").alias("cdi"),
            F.col("feature.properties.mmi").alias("mmi"),
            F.col("feature.properties.alert").alias("alert"),
            F.col("feature.properties.status").alias("status"),
            F.col("feature.properties.tsunami").alias("tsunami"),
            F.col("feature.properties.sig").alias("sig"),
            F.col("feature.properties.net").alias("net"),
            F.col("feature.properties.code").alias("code"),
            F.col("feature.properties.nst").alias("nst"),
            F.col("feature.properties.dmin").alias("dmin"),
            F.col("feature.properties.rms").alias("rms"),
            F.col("feature.properties.gap").alias("gap"),
            F.col("feature.properties.magType").alias("magType"),
            F.col("feature.properties.type").alias("type"),
            F.col("feature.properties.title").alias("title"),
            # GeoJSON coordinates are [longitude, latitude, depth]
            F.col("feature.geometry.coordinates").getItem(0).alias("longitude"),
            F.col("feature.geometry.coordinates").getItem(1).alias("latitude"),
            F.col("feature.geometry.coordinates").getItem(2).alias("depth")
        )
        record_count = df_bronze.count()
        logger.info(f"Successfully flattened raw data. Record count: {record_count}")
        
    except Exception as e:
        logger.error(f"Failed during Ingestion/Flattening phase. Error: {e}", exc_info=True)
        raise

    # --- 3. Transformation Phase (Clean, Validate, Deduplicate) ---
    logger.info("--- Step 3: Transformation Phase ---")
    
    # 3.1 Type Casting
    logger.info("Applying data type definitions...")
    df_cleaned = df_bronze.withColumn("event_timestamp_utc", (F.col("time") / 1000).cast(TimestampType())) \
        .withColumn("updated_timestamp_utc", (F.col("updated") / 1000).cast(TimestampType())) \
        .withColumn("magnitude", F.col("mag").cast(DoubleType())) \
        .withColumn("depth_km", F.col("depth").cast(DoubleType())) \
        .withColumn("tsunami_warning", (F.col("tsunami") == 1).cast(BooleanType())) \
        .withColumn("significance", F.col("sig").cast(IntegerType())) \
        .withColumn("felt_reports", F.col("felt").cast(IntegerType())) \
        .withColumn("nst_stations", F.col("nst").cast(IntegerType())) \
        .withColumn("rms_travel_time", F.col("rms").cast(DoubleType())) \
        .withColumn("gap_azimuthal", F.col("gap").cast(DoubleType()))

    # 3.2 Column Selection / Renaming
    df_selected = df_cleaned.select(
        F.col("id").alias("event_id"), 
        "event_timestamp_utc", 
        "updated_timestamp_utc", 
        "magnitude", 
        "depth_km",
        "latitude", 
        "longitude", 
        "place", 
        F.col("type").alias("event_type"), 
        "magType", 
        "tsunami_warning",
        "significance", 
        "felt_reports", 
        "nst_stations", 
        "rms_travel_time", 
        "gap_azimuthal", 
        "alert", 
        "status", 
        "url", 
        "title"
    )

    # 3.3 Data Quality Validation
    # Filtering out invalid records (e.g., out-of-bounds coordinates, null critical fields)
    logger.info("Applying data quality filters (validation)...")
    df_validated = df_selected.filter(
        (F.col("magnitude").isNotNull()) & (F.col("magnitude").between(-2.0, 10.0)) &
        (F.col("latitude").isNotNull()) & (F.col("latitude").between(-90.0, 90.0)) &
        (F.col("longitude").isNotNull()) & (F.col("longitude").between(-180.0, 180.0)) &
        (F.col("depth_km").isNotNull()) & (F.col("depth_km") >= 0) & (F.col("depth_km") < 1000) &
        (F.col("event_timestamp_utc").isNotNull()) & (F.col("event_id").isNotNull())
    )
    
    # 3.4 Logic for Deduplication
    # Strategy: Partition by 'event_id' and order by 'updated_timestamp_utc' descending.
    # Keep only the first row (most recent update).
    logger.info("Deduplicating records based on latest 'updated_timestamp_utc' per 'event_id'...")
    window_spec = Window.partitionBy("event_id").orderBy(F.col("updated_timestamp_utc").desc())
    df_deduplicated = df_validated.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")

    # --- 4. Feature Engineering Phase ---
    logger.info("--- Step 4: Feature Engineering ---")
    
    df_enriched = df_deduplicated \
        .withColumn("magnitude_category",
            F.when(F.col("magnitude") < 3.0, "Micro")
             .when(F.col("magnitude") < 4.0, "Minor")
             .when(F.col("magnitude") < 5.0, "Light")
             .when(F.col("magnitude") < 6.0, "Moderate")
             .when(F.col("magnitude") < 7.0, "Strong")
             .when(F.col("magnitude") < 8.0, "Major")
             .otherwise("Great")) \
        .withColumn("depth_category",
            F.when(F.col("depth_km") <= 70, "Shallow")
             .when(F.col("depth_km") <= 300, "Intermediate")
             .otherwise("Deep")) \
        .withColumn("hemisphere_ns", F.when(F.col("latitude") >= 0, "Northern").otherwise("Southern")) \
        .withColumn("hemisphere_ew", F.when(F.col("longitude") >= 0, "Eastern").otherwise("Western")) \
        .withColumn("year", F.year(F.col("event_timestamp_utc"))) \
        .withColumn("month", F.month(F.col("event_timestamp_utc"))) \
        .withColumn("day", F.dayofmonth(F.col("event_timestamp_utc"))) \
        .withColumn("hour", F.hour(F.col("event_timestamp_utc"))) \
        .withColumn("day_of_week", F.dayofweek(F.col("event_timestamp_utc"))) \
        .withColumn("extracted_region_detail", F.trim(F.regexp_extract(F.col("place"), r",\s*(.*)$", 1))) \
        .withColumn("extracted_country",
            F.when(F.col("extracted_region_detail") != "", F.col("extracted_region_detail"))
             .otherwise(F.trim(F.col("place")))) \
        .withColumn("silver_processing_timestamp_utc", F.current_timestamp())
    
    # --- 5. Data Loading Phase ---
    logger.info("--- Step 5: Writing Data to Silver Layer ---")
    logger.info(f"Writing mode: overwrite (partitioned by year, month)")
    
    try:
        df_enriched.write \
            .format("delta") \
            .mode("overwrite") \
            .partitionBy("year", "month") \
            .save(SILVER_PATH)
        
        logger.info(f"Successfully wrote data to: {SILVER_PATH}")
    except Exception as e:
        logger.error(f"Failed to write data to S3. Error: {e}", exc_info=True)
        raise
        
    logger.info("--- Job Finalized Successfully ---")

if __name__ == "__main__":
    main()
    job.commit()