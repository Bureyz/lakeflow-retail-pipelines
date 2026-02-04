-- --- Dim Loyalty (Gold) ---
CREATE OR REFRESH MATERIALIZED VIEW dim_loyalty
AS SELECT 
  loyalty_segment_id,
  loyalty_segment_description,
  unit_threshold,
  vip_row_id
FROM silver_loyalty;