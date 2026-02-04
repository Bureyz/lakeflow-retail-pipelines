import dlt as dp
from pyspark.sql.functions import *

@dp.table(
    comment="Fact Sales with Foreign Keys"
)
def fact_sales():
    sales = dp.read("silver_sales")
    customers = dp.read("silver_customers").where("__END_AT IS NULL")
    
    return sales.join(customers, ["customer_id"], "left") \
        .select(
            sales.customer_id,
            date_format(sales.order_datetime, 'yyyyMMdd').cast('int').alias("date_key"),
            sales.item.curr.alias("product_id"),
            customers.loyalty_segment.alias("loyalty_segment_id"),
            
            sales.item.qty.alias("quantity"),
            sales.item.price.alias("unit_price"),
            sales.total_row_price.alias("total_line_amount"),
            sales.order_datetime
        )