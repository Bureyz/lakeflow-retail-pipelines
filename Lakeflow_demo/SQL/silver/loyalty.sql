-- --- Silver Loyalty Segments (SCD Type 1) ---
-- Reference table, rarely changes.
CREATE OR REFRESH STREAMING TABLE silver_loyalty;

APPLY CHANGES INTO silver_loyalty
FROM STREAM(bronze_loyalty)
KEYS (loyalty_segment_id)
SEQUENCE BY source_timestamp
COLUMNS * EXCEPT (source_timestamp)
STORED AS SCD TYPE 1;