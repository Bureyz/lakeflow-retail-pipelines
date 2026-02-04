CREATE OR REFRESH STREAMING TABLE bronze_products
AS SELECT * FROM STREAM read_files(
  "/Volumes/lakeflow_demo/default/dataset/landing/products", 
  format => "csv", 
  header => "true", 
  inferSchema => "true"
);