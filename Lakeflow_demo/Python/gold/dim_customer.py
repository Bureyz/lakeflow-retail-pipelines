import databricks.pipelines as dp
from pyspark.sql.functions import col, when

@dp.table(
    name="dim_customer",
    comment="Dim Customer (Gold View)"
)
def dim_customer():
    return (
        dp.read("silver_customers")
        .select(
            "customer_id",
            "customer_name",
            "city",
            "state",
            "district",
            "postcode",
            "street",
            "lat",
            "lon",
            "unit",
            col("__START_AT").alias("valid_from"),
            col("__END_AT").alias("valid_to"),
            when(col("__END_AT").isNull(), True).otherwise(False).alias("is_current")
        )
    )
