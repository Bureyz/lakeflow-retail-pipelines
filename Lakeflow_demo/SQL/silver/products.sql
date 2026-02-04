-- --- Silver Products (SCD Type 1 Implementation) ---
-- Logic: Dumb overwrite for products.
CREATE OR REFRESH STREAMING TABLE silver_products;

APPLY CHANGES INTO silver_products
FROM STREAM(bronze_products)
KEYS (product_id)
SEQUENCE BY source_timestamp
COLUMNS * EXCEPT (source_timestamp)
STORED AS SCD TYPE 1;