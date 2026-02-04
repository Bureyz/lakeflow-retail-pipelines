import dlt as dp

# --- Silver Products (SCD Type 1) ---
dp.create_streaming_table("silver_products")

dp.apply_changes(
    target = "silver_products",
    source = "bronze_products",
    keys = ["product_id"],
    sequence_by = "source_timestamp",
    stored_as_scd_type = 1 
)