import databricks.pipelines as dp
from pyspark.sql.functions import col, expr

dp.create_streaming_table("silver_customers")

dp.apply_changes(
    target = "silver_customers",
    source = "bronze_customers",
    keys = ["customer_id"],
    sequence_by = col("ingestion_timestamp"),
    apply_as_delete_when = expr("operation = 'DELETE'"),
    except_column_list = ["operation", "ingestion_timestamp", "source_file_path", "source_file_name", "source_file_modification_time", "source_timestamp"],
    stored_as_scd_type = 2
)
