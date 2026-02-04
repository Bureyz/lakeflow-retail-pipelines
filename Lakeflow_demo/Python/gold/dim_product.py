import databricks.pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name="dim_product",
    comment="Dim Product (Gold View)"
)
def dim_product():
    return (
        dp.read("silver_products")
        .select(
            "product_id",
            "product_name",
            "product_category",
            "sales_price",
            "EAN13",
            "EAN5"
        )
    )
