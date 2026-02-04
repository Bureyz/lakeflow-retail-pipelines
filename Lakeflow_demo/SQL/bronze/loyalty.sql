CREATE OR REFRESH STREAMING TABLE bronze_loyalty
AS SELECT * FROM STREAM read_files(
  "/Volumes/lakeflow_demo/default/dataset/landing/loyalty_segments", 
  format => "csv", 
  header => "true", 
  inferSchema => "true"
);