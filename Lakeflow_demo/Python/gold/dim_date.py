import databricks.pipelines as dp
from pyspark.sql.functions import *

@dp.table(
    name="dim_date",
    comment="Dim Date (Generated)"
)
def dim_date():
    return (
        spark.range(0, 365*10)
        .select(date_add(lit('2020-01-01'), col("id").cast("int")).alias("calendar_date"))
        .select(
            date_format("calendar_date", 'yyyyMMdd').cast("int").alias("date_key"),
            "calendar_date",
            year("calendar_date").alias("year"),
            quarter("calendar_date").alias("quarter"),
            month("calendar_date").alias("month"),
            weekofyear("calendar_date").alias("week_of_year"),
            dayofweek("calendar_date").alias("day_of_week"),
            when(dayofweek("calendar_date").isin([1, 7]), True).otherwise(False).alias("is_weekend")
        )
    )
