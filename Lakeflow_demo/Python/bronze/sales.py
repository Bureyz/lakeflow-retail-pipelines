import dlt as dp

LANDING_PATH = "/Volumes/lakeflow_demo/default/dataset/landing/sales_orders"

@dp.table(
    comment="Raw sales JSON data (Auto Loader)"
)
def bronze_sales():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .load(LANDING_PATH)
    )