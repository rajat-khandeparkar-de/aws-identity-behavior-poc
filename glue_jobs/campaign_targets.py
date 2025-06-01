import sys
import logging

from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions

from pyspark.context import SparkContext
from pyspark.sql import functions as F

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

# Read churn scores
input_path = f"s3://aws-identity-behavior-poc-bucket/targets/churn_risk/dt={event_dt}/"
logger.info(f"Reading churn risk scores from: {input_path}")
df = spark.read.parquet(input_path)

# Segment: High Risk Customers
logger.info("Filtering high-risk customers...")
high_risk_df = df.filter(F.col("churn_risk") == "high_risk") \
    .select("unified_customer_id", "churn_risk", "last_event_ts")
logger.info("Sample high-risk customers:")
high_risk_df.show(10, truncate=False)

# Segment: Low Risk VIPs
logger.info("Filtering low-risk VIP customers (AOV > 50)...")
low_risk_vips_df = df.filter(
    (F.col("churn_risk") == "low_risk") & (F.col("average_order_value") > 50)
).select("unified_customer_id", "churn_risk", "average_order_value")
logger.info("Sample low-risk VIP customers:")
low_risk_vips_df.show(10, truncate=False)

# Export to CSV
base_output = f"s3://aws-identity-behavior-poc-bucket/campaigns/targeted_users/dt={event_dt}/"
logger.info(f"Writing high-risk customers to: {base_output}high_risk_customers")
high_risk_df.coalesce(1).write.mode("overwrite").option("header", True).csv(base_output + "high_risk_customers")

logger.info(f"Writing low-risk VIPs to: {base_output}low_risk_vips")
low_risk_vips_df.coalesce(1).write.mode("overwrite").option("header", True).csv(base_output + "low_risk_vips")

logger.info("Segmentation export completed.")
