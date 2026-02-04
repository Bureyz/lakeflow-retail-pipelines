-- --- Dim Date (Generated) ---
-- Standard Gregorian Calendar Dimension
CREATE OR REFRESH MATERIALIZED VIEW dim_date
AS 
WITH dates AS (
  SELECT date_add('2020-01-01', row_number() OVER(ORDER BY 1) - 1) as calendar_date
  FROM range(0, 365*10) -- Generate 10 years of dates
)
SELECT 
  int(date_format(calendar_date, 'yyyyMMdd')) as date_key,
  calendar_date,
  year(calendar_date) as year,
  quarter(calendar_date) as quarter,
  month(calendar_date) as month,
  weekofyear(calendar_date) as week_of_year,
  dayofweek(calendar_date) as day_of_week,
  case when dayofweek(calendar_date) in (1, 7) then true else false end as is_weekend
FROM dates;