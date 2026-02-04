import databricks.pipelines as dp
from pyspark.sql.functions import current_timestamp, col

@dp.table
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load("/Volumes/lakeflow_demo/default/dataset/landing/customers")
        .select(
            "*",
            col("_metadata.file_path").alias("source_file_path"),
            col("_metadata.file_name").alias("source_file_name"),
            col("_metadata.file_modification_time").alias("source_file_modification_time"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )
