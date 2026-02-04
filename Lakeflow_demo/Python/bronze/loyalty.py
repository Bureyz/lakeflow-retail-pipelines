import dlt as dp

LANDING_PATH = "/Volumes/lakeflow_demo/default/dataset/landing/loyalty_segments"

@dp.table(
    comment="Raw loyalty segments (Auto Loader)"
)
def bronze_loyalty():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(LANDING_PATH)
    )