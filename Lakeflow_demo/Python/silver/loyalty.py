import dlt as dp

# --- Silver Loyalty (SCD Type 1) ---
dp.create_streaming_table("silver_loyalty")

dp.apply_changes(
    target = "silver_loyalty",
    source = "bronze_loyalty",
    keys = ["loyalty_segment_id"],
    sequence_by = "source_timestamp",
    stored_as_scd_type = 1 
)