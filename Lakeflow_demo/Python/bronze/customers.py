import dlt as dp
from pyspark.sql.functions import *
from pyspark.sql.types import *

LANDING_PATH = "/Volumes/lakeflow_demo/default/dataset/landing/customers"

@dp.table(
    comment="Raw customers data (Auto Loader)"
)
def bronze_customers():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .load(LANDING_PATH)
    )