import sys
import logging

from pyspark.context import SparkContext
from pyspark.sql.functions import col, coalesce
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

args = getResolvedOptions(sys.argv, ["JOB_NAME", "event_dt"])
sc = SparkContext()
gc = GlueContext(sc)

spark = gc.spark_session
job = Job(gc)
job.init(args["JOB_NAME"], args)

event_dt = args["event_dt"]
logger.info(f"Glue job started with event_dt = {event_dt}")

# Load CRM Data
logger.info("Loading CRM data from Glue Catalog...")
crm_dyf = gc.create_dynamic_frame.from_catalog(
    database="aws_identity_behavior_customer_data",
    table_name="crm"
)
crm_df = crm_dyf.toDF()
logger.info("Sample CRM data:")
crm_df.show(5, truncate=False)

# Load Events Data from S3
events_path = f"s3://aws-identity-behavior-poc-bucket/raw/events/dt={event_dt}/"
logger.info(f"Loading events data from path: {events_path}")
events_df = spark.read.json(events_path)
logger.info("Sample Events data:")
events_df.show(5, truncate=False)


# Alias the DataFrames
events = events_df.alias("events")
crm_device = crm_df.alias("crm_device")
crm_email = crm_df.alias("crm_email")
crm_phone = crm_df.alias("crm_phone")

# Join
logger.info("Joining Events with CRM data on device_id...")
# First join on device_id
joined_df = events.join(crm_device, col("events.device_id") == col("crm_device.device_id"), "left") \
    .withColumn("customer_id_device", col("crm_device.customer_id")) \
    .drop(col("crm_device.device_id"))

# Second join on email
joined_df = joined_df.join(crm_email, col("events.email") == col("crm_email.email"), "left") \
    .withColumn("customer_id_email", col("crm_email.customer_id")) \
    .drop(col("crm_email.email"))

logger.info("Sample joined data:")
joined_df.show(5, truncate=False)

# Coalesce to pick first matched customer_id
final_df = joined_df.withColumn(
    "unified_customer_id",
    coalesce("customer_id_device", "customer_id_email")
)
logger.info("Sample Final data:")
final_df.show(5, truncate=False)
logger.info("Job complete. Committing...")
job.commit()
logger.info("Glue job finished successfully.")

# Write resolved output to S3 (Parquet)
output_path = f"s3://aws-identity-behavior-poc-bucket/clean/events_resolved/dt={event_dt}/"
final_df.write.mode("overwrite").partitionBy("market").parquet(output_path)

job.commit()
