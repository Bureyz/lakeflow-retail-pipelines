import dlt as dp

@dp.table(
    comment="Dim Product (Gold View)"
)
def dim_product():
    return dp.read("silver_products").select(
        "product_id",
        "product_name",
        "category",
        "price",
        "description"
    )