import dlt as dp

@dp.table(comment="Dim Loyalty (Gold)")
def dim_loyalty():
    return dp.read("silver_loyalty").select(
        "loyalty_segment_id",
        "loyalty_segment_description",
        "unit_threshold",
        "vip_row_id"
    )