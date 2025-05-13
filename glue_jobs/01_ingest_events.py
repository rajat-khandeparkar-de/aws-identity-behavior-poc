import sys
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.utils import getResolvedOptions
from pyspark.sql.functions import *

# Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Input/output S3 paths
input_path = "s3://aws-identity-behavior/customer-events/events_sample.csv"
output_path = "s3://aws-identity-behavior/customer-events-parquet/"

# Read from S3
df = spark.read.option("header", True).csv(input_path)

# Convert timestamp column
df = df.withColumn("timestamp", to_timestamp("timestamp"))

# Write as Parquet
df.write.mode("overwrite").parquet(output_path)

print(f"✅ Ingested events written to S3: {output_path}")
