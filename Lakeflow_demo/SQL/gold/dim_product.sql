-- --- Dim Product (Gold View) ---
CREATE OR REFRESH MATERIALIZED VIEW dim_product
AS SELECT 
  product_id,
  product_name,
  category,
  price,
  description
FROM silver_products;