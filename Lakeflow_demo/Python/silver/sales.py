import databricks.pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name="silver_sales",
    comment="Silver Sales (Cleaned Fact Source)"
)
@dp.expect_or_drop("valid_customer_id", "customer_id IS NOT NULL")
@dp.expect_or_drop("valid_order_datetime", "order_datetime IS NOT NULL")
@dp.expect_or_drop("valid_product_id", "product_id IS NOT NULL")
@dp.expect_or_drop("positive_quantity", "quantity > 0")
@dp.expect_or_drop("positive_unit_price", "unit_price >= 0")
@dp.expect("valid_order_number", "order_number IS NOT NULL")
def silver_sales():
    return (
        dp.read_stream("bronze_sales").alias("s")
        .join(
            dp.read("silver_customers").alias("c"),
            (col("s.customer_id") == col("c.customer_id")) & (col("c.__END_AT").isNull()),
            "left"
        )
        .join(
            dp.read("silver_loyalty").alias("l"),
            col("c.loyalty_segment") == col("l.loyalty_segment_id"),
            "left"
        )
        .select(
            "s.order_number",
            "s.customer_id",
            "s.customer_name",
            "s.order_datetime",
            "s.number_of_line_items",
            "s.product_id",
            "s.product_name",
            "s.unit_price",
            "s.quantity",
            "s.currency",
            "s.unit",
            col("c.loyalty_segment").alias("loyalty_segment_id"),
            "l.loyalty_segment_description"
        )
    )
