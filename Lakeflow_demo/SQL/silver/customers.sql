-- --- Silver Customers (SCD Type 2 Implementation) ---
-- Logic: We track history here technically.
CREATE OR REFRESH STREAMING TABLE silver_customers;

APPLY CHANGES INTO silver_customers
FROM STREAM(bronze_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY ingestion_timestamp
COLUMNS * EXCEPT (operation, ingestion_timestamp, source_file_path, source_file_name, source_file_modification_time, source_timestamp)
STORED AS SCD TYPE 2;
