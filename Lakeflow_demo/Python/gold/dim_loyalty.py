import databricks.pipelines as dp
from pyspark.sql.functions import col

@dp.table(
    name="dim_loyalty",
    comment="Dim Loyalty (Gold)"
)
def dim_loyalty():
    return (
        dp.read("silver_loyalty")
        .select(
            "loyalty_segment_id",
            "loyalty_segment_description",
            "unit_threshold",
            "valid_from",
            "valid_to"
        )
    )
