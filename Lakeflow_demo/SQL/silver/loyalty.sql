-- --- Silver Loyalty Segments (SCD Type 1) ---
-- Reference table, rarely changes.
CREATE OR REFRESH STREAMING TABLE silver_loyalty;

APPLY CHANGES INTO silver_loyalty
FROM STREAM(bronze_loyalty)
KEYS (loyalty_segment_id)
SEQUENCE BY ingestion_timestamp
COLUMNS * EXCEPT (ingestion_timestamp, source_file_path, source_file_name, source_file_modification_time)
STORED AS SCD TYPE 1;
