import os
from pyspark.sql import SparkSession

def revome_prefix_from_columns(df, prefix):
    for col in df.columns:
        if col.startswith(prefix):
            df = df.withColumnRenamed(col, col.replace(prefix, ""))
    return df

# Create sparkSession
spark = SparkSession.builder \
    .appName("SilverCustomers") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()   
    

# Input PARQUET path
# AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
# PROJECT_PATH = os.path.dirname(AIRFLOW_HOME)

PROJECT_PATH = os.getenv("S3_PATH")
input_path = f"{PROJECT_PATH}/lakehouse/bronze/customers.parquet"

# Exit route in Parquet format
output_path = f"{PROJECT_PATH}/lakehouse/silver/customers.parquet"

# Read parquet
df = spark.read.parquet(input_path)
df = revome_prefix_from_columns(df, "customer_")
df.write.mode("overwrite").parquet(output_path)

spark.stop()