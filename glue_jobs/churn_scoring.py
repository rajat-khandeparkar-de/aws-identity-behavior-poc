import sys
import logging

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F
from datetime import datetime, timedelta

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
formatter = logging.Formatter('[%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Log start
logger.info("Starting churn risk scoring job...")

args = getResolvedOptions(sys.argv, ['event_dt'])
event_dt = args['event_dt']

sc = SparkContext()
gc = GlueContext(sc)

spark = gc.spark_session

# Read features
features_path = f"s3://aws-identity-behavior-poc-bucket/features/customer_behavior/dt={event_dt}/"
logger.info(f"Reading features from: {features_path}")
features_df = spark.read.parquet(features_path)
logger.info("Sample of loaded features:")
features_df.show(5, truncate=False)

# Define rules for churn risk
logger.info("Applying churn risk rules...")
threshold_date = datetime.now() - timedelta(days=14)

scored_df = features_df.withColumn(
    "churn_risk",
    F.when(F.col("total_orders") < 2, "high_risk")
    .when(F.col("last_event_ts") < F.lit(threshold_date.isoformat()), "high_risk")
    .when(F.col("average_order_value") < 10, "medium_risk")
    .otherwise("low_risk")
)
logger.info("Sample of churn risk scoring:")
scored_df.show(20, truncate=False)

# Write scored output to S3
output_path = f"s3://aws-identity-behavior-poc-bucket/targets/churn_risk/dt={event_dt}/"
logger.info(f"Writing churn risk scores to: {output_path}")
scored_df.write.mode("overwrite").parquet(output_path)

logger.info("Churn scoring job completed successfully.")
