import dlt as dp
from pyspark.sql.functions import expr

# --- Silver Customers (SCD Type 2) ---
dp.create_streaming_table("silver_customers")
# Note: apply_changes is still provided by the module, alias handles it
dp.apply_changes(
    target = "silver_customers",
    source = "bronze_customers",
    keys = ["customer_id"],
    sequence_by = "source_timestamp",
    apply_as_deletes = expr("operation = 'DELETE'"),
    except_column_list = ["operation", "source_timestamp"],
    stored_as_scd_type = 2 
)