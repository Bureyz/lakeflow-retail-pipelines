import dlt as dp

LANDING_PATH = "/Volumes/lakeflow_demo/default/dataset/landing/products"

@dp.table(
    comment="Raw products data (Auto Loader)"
)
def bronze_products():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(LANDING_PATH)
    )