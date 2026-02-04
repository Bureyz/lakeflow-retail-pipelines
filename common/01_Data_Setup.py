# Databricks notebook source
# MAGIC %md
# MAGIC # Common Data Setup for Lakeflow Demo
# MAGIC 
# MAGIC This script simulates a realistic "Landing Zone" for our Lakeflow Pipeline. 
# MAGIC Since built-in datasets are static (`/databricks-datasets/retail-org`), we need to:
# MAGIC 1. Copy them to a writable location (`/tmp/demo/lakeflow/...`).
# MAGIC 2. Generate "Updates" to simulate Slowly Changing Dimensions (SCD Type 2).
# MAGIC 3. Simulate "New Data" arriving for Streaming ingestion.
# MAGIC 
# MAGIC **Run this notebook ONCE before starting the DLT/Lakeflow Pipeline.**

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *

# Configuration
SOURCE_PATH = "/databricks-datasets/retail-org"
DEST_PATH = "/Volumes/lakeflow_demo/default/dataset/landing"
RESET_DATA = True

# Paths for Landing Zone
paths = {
    "customers": f"{DEST_PATH}/customers",
    "sales": f"{DEST_PATH}/sales_orders",
    "products": f"{DEST_PATH}/products",
    "loyalty": f"{DEST_PATH}/loyalty_segments"
}

# Clean start
if RESET_DATA:
    dbutils.fs.rm(DEST_PATH, True)
    print(f"Cleaned up {DEST_PATH}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Initial "Fillup" (Backfill)
# MAGIC Copying the base historical data. This represents the data present before we switch to real-time.

# COMMAND ----------

# --- Customers (CSV) ---
# Reading original customers
df_cust = spark.read.option("header", True).csv(f"{SOURCE_PATH}/customers")

# Adding a timestamp to simulate when this data "arrived" (Scenario: 1 year ago)
df_cust_init = df_cust.withColumn("operation", F.lit("INSERT")) \
                      .withColumn("source_timestamp", F.expr("current_timestamp() - interval 365 days"))

# Write as generic CSVs to landing zone
df_cust_init.write.format("csv").option("header", True).mode("append").save(paths["customers"])
print(f"Copied {df_cust_init.count()} customers to {paths['customers']}")

# --- Products (CSV) ---
df_prod = spark.read.option("header", True).csv(f"{SOURCE_PATH}/products")
df_prod.withColumn("source_timestamp", F.expr("current_timestamp() - interval 365 days")) \
       .write.format("csv").option("header", True).mode("append").save(paths["products"])
print(f"Copied products to {paths['products']}")

# --- Loyalty Segments (CSV) ---
df_loyalty = spark.read.option("header", True).csv(f"{SOURCE_PATH}/loyalty_segments")
df_loyalty.write.format("csv").option("header", True).mode("append").save(paths["loyalty"])
print(f"Copied loyalty segments to {paths['loyalty']}")

# --- Sales Orders (JSON) ---
# Reading original sales
df_sales = spark.read.json(f"{SOURCE_PATH}/sales_orders")

# Write a subset as "History"
df_sales.limit(1000).write.format("json").mode("append").save(paths["sales"])
print(f"Copied initial 1000 sales orders to {paths['sales']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Simulate Updates (SCD Type 2)
# MAGIC We will pick some customers and "update" their city or loyalty tier.
# MAGIC This allows `APPLY CHANGES INTO` to detect changes and close history records.

# COMMAND ----------

# Select 10 customers to update
updates_df = df_cust_init.limit(10) \
    .withColumn("operation", F.lit("UPDATE")) \
    .withColumn("source_timestamp", F.expr("current_timestamp()")) \
    .withColumn("city", F.lit("San Francisco")) \
    .withColumn("state", F.lit("CA")) \
    .withColumn("street", F.lit("123 Lakeflow Blvd"))

# Append these "newer" records to the SAME source folder
updates_df.write.format("csv").option("header", True).mode("append").save(paths["customers"])
print("Generated 10 customer updates (SCD Type 2 simulation).")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Simulate New Sales (Streaming)
# MAGIC Adding more files to `sales_orders` to show Cloud Files (Auto Loader) picking them up.

# COMMAND ----------

# Pick another chunk of sales and save as "New Data"
df_sales_new = df_sales.orderBy(F.rand()).limit(500)
df_sales_new.write.format("json").mode("append").save(paths["sales"])
print("Generated 500 new sales orders.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Data Readiness
# MAGIC 
# MAGIC Data is now ready at:
# MAGIC - `customers`: `/Volumes/lakeflow_demo/default/dataset/landing/customers/` (Contains History + Updates)
# MAGIC - `products`: `/Volumes/lakeflow_demo/default/dataset/landing/products/`
# MAGIC - `sales`: `/Volumes/lakeflow_demo/default/dataset/landing/sales_orders/`
# MAGIC - `loyalty`: `/Volumes/lakeflow_demo/default/dataset/landing/loyalty_segments/`
# MAGIC 
# MAGIC You can now run the Lakeflow Pipeline (SQL or Python).
