import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import countDistinct, count, sum as _sum


# Create sparkSession
spark = SparkSession.builder \
    .appName("GoldLayer") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .getOrCreate()
    
    
# Input PARQUET path
# AIRFLOW_HOME = os.getenv("AIRFLOW_HOME")
# PROJECT_PATH = os.path.dirname(AIRFLOW_HOME)

# Read data from the Silver layer
PROJECT_PATH = os.getenv("S3_PATH")
customers = spark.read.parquet(f"{PROJECT_PATH}/lakehouse/silver/customers.parquet")
orders = spark.read.parquet(f"{PROJECT_PATH}/lakehouse/silver/orders.parquet")
order_item = spark.read.parquet(f"{PROJECT_PATH}/lakehouse/silver/order_item.parquet")

# Join customers with orders
orders_with_customers = orders.join(customers, orders.customer_id == customers.id).select(
            orders["id"].alias("order_id"),
            orders["customer_id"],            
            customers["city"],
            customers["state"]
        )

# Join with order_item
# Note: Some orders don-t have items(left join)
full_data = orders_with_customers.join(order_item, orders_with_customers.order_id == order_item.order_id, "left").select(
            orders_with_customers["order_id"],
            order_item["subtotal"],
            orders_with_customers["city"],
            orders_with_customers["state"]
        )


#full_data.filter(full_data.city == "Norwalk").show(truncate=False)


# Grouping and calculation
summary = full_data.groupBy("city", "state") \
    .agg(
        countDistinct("order_id").alias("order_quantity"),
        _sum("subtotal").alias("total_order_value")
    )

# Save result in Parquet
summary.write.mode("overwrite").parquet(f"{PROJECT_PATH}/lakehouse/gold/summary_orders.parquet")

# Show ten rows
summary.show(10, truncate=False)

spark.stop()
