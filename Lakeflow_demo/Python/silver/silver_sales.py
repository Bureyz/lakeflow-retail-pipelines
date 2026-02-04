import dlt as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

# UDF for masking PII
@udf(returnType=StringType())
def mask_name(name):
    if name and len(name) > 3:
        return name[0] + "*" * (len(name)-2) + name[-1]
    return name

@dp.table(
    comment="Cleaned sales with PII masking"
)
@dp.expect_or_drop("valid_order", "number_of_line_items > 0")
def silver_sales():
    return dp.read_stream("bronze_sales") \
        .select(
            col("customer_id"),
            mask_name(col("customer_name")).alias("customer_name_masked"),
            col("order_datetime"),
            col("clicked_items"),
            explode(col("ordered_products")).alias("item")
        ) \
        .withColumn("total_row_price", col("item.qty") * col("item.price"))