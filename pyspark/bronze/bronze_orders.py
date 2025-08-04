import os
from pyspark.sql import SparkSession

# Create sparkSession
spark = SparkSession.builder \
    .appName("BronzeOrders") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()

# Input JSON path
# AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
# PROJECT_PATH = os.path.dirname(AIRFLOW_HOME)
PROJECT_PATH = os.getenv("S3_PATH")

input_path = f"{PROJECT_PATH}/lakehouse/landing/orders.json"

# Exit route in Parquet format
output_path = f"{PROJECT_PATH}/lakehouse/bronze/orders.parquet"

# read JSON
df = spark.read.json(input_path)

# Writing like Parquet
df.write.mode("overwrite").parquet(output_path)

print("✅ Bronze transformation completed: JSON → Parquet")

spark.stop()