import databricks.pipelines as dp
from pyspark.sql.functions import col, date_format, lit

@dp.table(
    name="fact_sales",
    comment="Fact Sales"
)
def fact_sales():
    return (
        dp.read("silver_sales").alias("s")
        .join(
            dp.read("silver_customers").alias("c"),
            (col("s.customer_id") == col("c.customer_id")) & (col("c.__END_AT").isNull()),
            "left"
        )
        .select(
            col("s.customer_id"),
            date_format(col("s.order_datetime"), 'yyyyMMdd').cast("int").alias("date_key"),
            col("s.product_id"),
            col("c.loyalty_segment").alias("loyalty_segment_id"),
            col("s.quantity"),
            col("s.unit_price"),
            (col("s.quantity") * col("s.unit_price")).alias("total_line_amount"),
            col("s.order_datetime")
        )
    )
