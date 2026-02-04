-- --- Silver Customers (SCD Type 2 Implementation) ---
-- Logic: We track history here technically.
CREATE OR REFRESH STREAMING TABLE silver_customers;

APPLY CHANGES INTO silver_customers
FROM STREAM(bronze_customers)
KEYS (customer_id)
APPLY AS DELETE WHEN operation = 'DELETE'
SEQUENCE BY source_timestamp
COLUMNS * EXCEPT (operation, source_timestamp)
STORED AS SCD TYPE 2;