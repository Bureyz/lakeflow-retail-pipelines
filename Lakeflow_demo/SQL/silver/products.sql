-- --- Silver Products (SCD Type 1 Implementation) ---
-- Logic: Dumb overwrite for products.
CREATE OR REFRESH STREAMING TABLE silver_products;

APPLY CHANGES INTO silver_products
FROM STREAM(bronze_products)
KEYS (product_id)
SEQUENCE BY ingestion_timestamp
COLUMNS * EXCEPT (ingestion_timestamp, source_file_path, source_file_name, source_file_modification_time)
STORED AS SCD TYPE 1;
