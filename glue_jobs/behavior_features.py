import sys
import logging
from datetime import datetime, timedelta

from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.window import Window

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

args = getResolvedOptions(sys.argv, ['event_dt'])
event_dt = args['event_dt']

sc = SparkContext()
gc = GlueContext(sc)
spark = gc.spark_session

logger.info(f"Starting customer behavior feature job for dt={event_dt}")
input_path = f"s3://aws-identity-behavior-poc-bucket/clean/events_resolved/dt={event_dt}/"
logger.info(f"Reading input data from {input_path}")
events_df = spark.read.parquet(input_path)


logger.info("Filtering events for last 30 days...")
thirty_days_ago = datetime.now() - timedelta(days=30)
events_df = events_df.withColumn("timestamp", F.to_timestamp("timestamp")) \
                     .filter(F.col("timestamp") >= F.lit(thirty_days_ago))
events_df.show(20, truncate=False)


logger.info("Generating features...")

# Total Orders
logger.info("Computing total_orders...")
order_count = events_df.filter(F.col("event_type") == "order_placed") \
    .groupBy("unified_customer_id") \
    .agg(F.count("*").alias("total_orders"))
order_count.show(20, truncate=False)

# Avg Order Value
logger.info("Computing avg_order_value...")
avg_order_value = events_df.filter(F.col("event_type") == "order_placed") \
    .groupBy("unified_customer_id") \
    .agg(F.avg("amount").alias("average_order_value"))
avg_order_value.show(20, truncate=False)

# Last Seen Timestamp
logger.info("Computing last_event_ts...")
last_event = events_df.groupBy("unified_customer_id") \
    .agg(F.max("timestamp").alias("last_event_ts"))
last_event.show(20, truncate=False)

# Event Frequency by Hour
logger.info("Computing top_event_hour...")
hourly_mode = events_df.withColumn("event_hour", F.hour("timestamp")) \
    .groupBy("unified_customer_id", "event_hour") \
    .agg(F.count("*").alias("count")) \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("unified_customer_id").orderBy(F.desc("count")))) \
    .filter(F.col("rank") == 1) \
    .select("unified_customer_id", F.col("event_hour").alias("top_event_hour"))
hourly_mode.show(20, truncate=False)

# Most Frequent Event Type
logger.info("Computing top_event_type...")
top_event_type = events_df.groupBy("unified_customer_id", "event_type") \
    .agg(F.count("*").alias("cnt")) \
    .withColumn("rank", F.row_number().over(
        Window.partitionBy("unified_customer_id").orderBy(F.desc("cnt")))) \
    .filter(F.col("rank") == 1) \
    .select("unified_customer_id", F.col("event_type").alias("top_event_type"))
top_event_type.show(20, truncate=False)

# Join all features
logger.info("Joining all features into single DataFrame...")
features_df = order_count \
    .join(avg_order_value, "unified_customer_id", "outer") \
    .join(last_event, "unified_customer_id", "outer") \
    .join(hourly_mode, "unified_customer_id", "outer") \
    .join(top_event_type, "unified_customer_id", "outer")

logger.info("Sample features output:")
features_df.show(5, truncate=False)

# Write to output
output_path = f"s3://aws-identity-behavior-poc-bucket/features/customer_behavior/dt={event_dt}/"
logger.info(f"Writing features to {output_path}")
features_df.write.mode("overwrite").parquet(output_path)

logger.info("Feature job completed successfully.")
