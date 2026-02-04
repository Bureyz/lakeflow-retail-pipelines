import databricks.pipelines as dp
from pyspark.sql.functions import col

dp.create_streaming_table("silver_loyalty")

dp.apply_changes(
    target = "silver_loyalty",
    source = "bronze_loyalty",
    keys = ["loyalty_segment_id"],
    sequence_by = col("ingestion_timestamp"),
    except_column_list = ["ingestion_timestamp", "source_file_path", "source_file_name", "source_file_modification_time"],
    stored_as_scd_type = 1
)
