-- --- Silver Sales (Cleaned Fact Source) ---
-- Key concept: Flattener array & Data Quality expectations
CREATE OR REFRESH STREAMING TABLE silver_sales (
  CONSTRAINT valid_amount EXPECT (clicked_items IS NOT NULL) ON VIOLATION DROP ROW
)
AS SELECT 
  customer_id,
  customer_name,
  order_datetime,
  explode(ordered_products) as item, -- Array explode
  number_of_line_items,
  clicked_items
FROM STREAM(bronze_sales);