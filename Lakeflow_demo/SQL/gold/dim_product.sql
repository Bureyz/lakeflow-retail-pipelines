-- --- Dim Product (Gold View) ---
CREATE OR REFRESH MATERIALIZED VIEW dim_product
AS SELECT 
  product_id,
  product_name,
  product_category,
  sales_price,
  EAN13,
  EAN5
FROM silver_products;