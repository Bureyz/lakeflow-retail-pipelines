-- --- Fact Sales ---
-- Central Fact Table with Foreign Keys
CREATE OR REFRESH MATERIALIZED VIEW fact_sales
AS SELECT 
  -- Surrogate Keys / Foreign Keys
  s.customer_id, -- Link to Dim Customer
  int(date_format(s.order_datetime, 'yyyyMMdd')) as date_key, -- Link to Dim Date
  s.product_id, -- Link to Dim Product
  c.loyalty_segment as loyalty_segment_id, -- Link to Dim Loyalty

  -- Facts / Measures
  s.quantity,
  s.unit_price,
  (s.quantity * s.unit_price) as total_line_amount,
  
  -- Degenerate Dimensions (Timestamp)
  s.order_datetime
  
FROM silver_sales s
-- Join to get Customer attributes valid AT THE TIME of sale
LEFT JOIN silver_customers c 
  ON s.customer_id = c.customer_id 
  AND c.__END_AT IS NULL;
