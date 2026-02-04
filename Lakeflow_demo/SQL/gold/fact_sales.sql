-- --- Fact Sales ---
-- Central Fact Table with Foreign Keys
CREATE OR REFRESH MATERIALIZED VIEW fact_sales
AS SELECT 
  -- Surrogate Keys / Foreign Keys
  s.customer_id, -- Link to Dim Customer
  int(date_format(s.order_datetime, 'yyyyMMdd')) as date_key, -- Link to Dim Date
  s.item.curr as product_id, -- Link to Dim Product
  
  -- We assume 'loyalty_segment' in customers table maps to dim_loyalty
  -- But usually, we store the ID. Here we pass through valid attributes.
  c.loyalty_segment as loyalty_segment_id, 

  -- Facts / Measures
  s.item.qty as quantity,
  s.item.price as unit_price,
  (s.item.qty * s.item.price) as total_line_amount,
  
  -- Degenerate Dimensions (Timestamp)
  s.order_datetime
  
FROM silver_sales s
-- Join to get Customer attributes valid AT THE TIME of sale
LEFT JOIN silver_customers c 
  ON s.customer_id = c.customer_id 
  AND c.__END_AT IS NULL;