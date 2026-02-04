CREATE OR REFRESH STREAMING TABLE bronze_customers
AS SELECT * FROM STREAM read_files(
  "/Volumes/lakeflow_demo/default/dataset/landing/customers", 
  format => "csv", 
  header => "true", 
  inferSchema => "true"
);