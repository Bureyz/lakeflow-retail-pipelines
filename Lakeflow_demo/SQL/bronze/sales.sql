CREATE OR REFRESH STREAMING TABLE bronze_sales
AS SELECT * FROM STREAM read_files(
  "/Volumes/lakeflow_demo/default/dataset/landing/sales_orders", 
  format => "json", 
  inferSchema => "true"
);