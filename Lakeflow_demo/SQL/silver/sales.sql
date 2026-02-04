-- --- Silver Sales (Cleaned Fact Source) ---
-- Key concept: Flattener array & Data Quality expectations
CREATE OR REFRESH STREAMING TABLE silver_sales (
  CONSTRAINT valid_customer_id EXPECT (customer_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_datetime EXPECT (order_datetime IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT valid_product_id EXPECT (product_id IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT positive_quantity EXPECT (quantity > 0) ON VIOLATION DROP ROW,
  CONSTRAINT positive_unit_price EXPECT (unit_price >= 0) ON VIOLATION DROP ROW,
  CONSTRAINT valid_order_number EXPECT (order_number IS NOT NULL)
)
AS SELECT 
 s.order_number,
 s.customer_id,
 s.customer_name,
 s.order_datetime,
 s.number_of_line_items,
 s.product_id,
 s.product_name,
 s.unit_price,
 s.quantity,
 s.currency,
 s.unit,
 c.loyalty_segment as loyalty_segment_id,
 l.loyalty_segment_description
FROM STREAM bronze_sales s
LEFT JOIN silver_customers c
  ON s.customer_id = c.customer_id
  AND c.__END_AT IS NULL
LEFT JOIN silver_loyalty l
  ON c.loyalty_segment = l.loyalty_segment_id;
