import dlt as dp
from pyspark.sql.functions import col, when

@dp.table(
    comment="Dim Customer (Exposure Layer)"
)
def dim_customer():
    return dp.read("silver_customers").select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "city",
        "state",
        "loyalty_segment",
        col("__START_AT").alias("valid_from"),
        col("__END_AT").alias("valid_to"),
        when(col("__END_AT").isNull(), True).otherwise(False).alias("is_current")
    )