import dlt as dp
from pyspark.sql.functions import *

@dp.table(comment="Generated Date Dimension")
def dim_date():
    # Helper to generate range of dates
    return (
        spark.range(0, 365 * 10)
        .withColumn("calendar_date", date_add(lit("2020-01-01"), col("id").cast("int")))
        .select(
            col("calendar_date"),
            date_format("calendar_date", "yyyyMMdd").cast("int").alias("date_key"),
            year("calendar_date").alias("year"),
            quarter("calendar_date").alias("quarter"),
            month("calendar_date").alias("month"),
            weekofyear("calendar_date").alias("week_of_year"),
            dayofweek("calendar_date").alias("day_of_week"),
            when(dayofweek("calendar_date").isin([1, 7]), True).otherwise(False).alias("is_weekend")
        )
    )