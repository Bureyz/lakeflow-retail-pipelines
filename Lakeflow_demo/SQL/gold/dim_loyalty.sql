-- --- Dim Loyalty (Gold) ---
CREATE OR REFRESH MATERIALIZED VIEW dim_loyalty
AS SELECT 
  loyalty_segment_id,
  loyalty_segment_description,
  unit_threshold,
  valid_from,
  valid_to
FROM silver_loyalty;
