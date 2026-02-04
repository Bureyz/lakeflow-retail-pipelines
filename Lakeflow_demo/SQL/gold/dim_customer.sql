-- --- Dim Customer (Gold View) ---
-- The public contract for BI tools. 
CREATE OR REFRESH MATERIALIZED VIEW dim_customer
AS SELECT 
  customer_id,
  first_name,
  last_name,
  email,
  city,
  state,
  loyalty_segment,
  __START_AT as valid_from,
  __END_AT as valid_to,
  CASE WHEN __END_AT IS NULL THEN true ELSE false END as is_current
FROM silver_customers;