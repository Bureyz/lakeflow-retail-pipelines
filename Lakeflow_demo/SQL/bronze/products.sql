CREATE OR REFRESH STREAMING TABLE bronze_products
AS SELECT 
  *,
  _metadata.file_path as source_file_path,
  _metadata.file_name as source_file_name,
  _metadata.file_modification_time as source_file_modification_time,
  current_timestamp() as ingestion_timestamp
FROM STREAM read_files(
  "/Volumes/lakeflow_demo/default/dataset/landing/products", 
  format => "json", 
  inferSchema => true
);
