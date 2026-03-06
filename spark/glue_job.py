"""
AWS Glue/Spark ETL script for the e2e-de pipeline.

Loads cleaned silver data for the supplied run_date from S3, renames
columns, casts types, derives metrics, checks that row counts
are unchanged, and writes parquet to the gold prefix partitioned by
run_date & location.

Expected arguments: JOB_NAME, run_date, silver_bucket_prefix,
gold_bucket_prefix.
"""
import sys
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import col, round, when, to_timestamp, lit
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# --- argument parsing ---
args = getResolvedOptions(sys.argv, [
    'JOB_NAME',
    'run_date',
    'silver_bucket_prefix', 
    'gold_bucket_prefix'    
])

# --- spark/glue context setup --- 
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

job = Job(glueContext)
job.init(args['JOB_NAME'], args)

logger = glueContext.get_logger()
logger.info(f"Starting Glue Job: {args['JOB_NAME']}")
logger.info(f"Processing for run_date: {args['run_date']}")

# --- read silver data from S3 --- 

# remove possible trailing "/" from bucket prefix argument
silver_path = f"{args['silver_bucket_prefix'].rstrip('/')}/run_date={args['run_date']}/"

logger.info(f"Reading Silver Parquet data from: {silver_path}")

try:
    raw_df = spark.read.parquet(silver_path)
    record_count = raw_df.count()
    print(f"Successfully loaded {record_count} records from the Silver layer.")

except Exception as e:
    logger.error(f"Failed to read data from {silver_path}. Error: {str(e)}")
    sys.exit(1) 

# --- transformation logic ---
logger.info("Starting data transformations...")

curated_df = (
    raw_df
    # rename API fields to analytics ready names
    .withColumnRenamed("temperature_2m", "temp_celsius")
    .withColumnRenamed("relative_humidity_2m", "humidity_percent")
    .withColumnRenamed("wind_speed_10m", "wind_speed_kmh")
    .withColumnRenamed("precipitation", "precipitation_mm")
    
    # cast time string to TimestampType for partitioning/sorting
    .withColumn("time", to_timestamp(col("time")))
    
    # derived columns: fahrenheit & freezing flag
    .withColumn("temp_fahrenheit", round((col("temp_celsius") * 9/5) + 32, 2))
    .withColumn("is_freezing", when(col("temp_celsius") <= 0, True).otherwise(False))
)

logger.info("Transformations applied successfully.")

# --- row count validation ---
gold_count = curated_df.count()
print(f"Gold layer record count: {gold_count}")

if record_count != gold_count:
    logger.error(f"Validation failed! Silver count ({record_count}) != Gold count ({gold_count}).")
    sys.exit(1)
else:
    print("Validation passed: Silver and Gold record counts match.")

# --- configure and execute write to gold parquet ---
logger.info("Starting data load to Gold layer...")

# add run date columns for partitioning
final_df = curated_df.withColumn("run_date", lit(args['run_date']))
logger.info("Successfully added run_date column.")

# remove possible trailing "/" from bucket prefix argument
gold_path = args['gold_bucket_prefix'].rstrip('/')

try:
    (
        final_df.write

        # idempotent write
        .mode("overwrite")

        # bucket folder layout
        .partitionBy("run_date", "location")

        .parquet(gold_path)
    )
    logger.info(f"Successfully wrote curated Parquet data to {gold_path}")

except Exception as e:
    logger.error(f"Failed to write data to Gold layer: {str(e)}")
    sys.exit(1)

logger.info("All transforms written, ready to commit job")
job.commit()
