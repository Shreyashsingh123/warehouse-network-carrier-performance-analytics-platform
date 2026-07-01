from pyspark.sql.functions import col


def create_dimension_tables(
    final_df,
    capacity,
    cost,
    products,
    freight
):

    dim_warehouses = (
        capacity
        .join(
            cost,
            on="plant_code",
            how="left"
        )
        .select(
            col("plant_code").alias("plant_id"),
            "daily_capacity",
            "unit_storage_cost"
        )
        .dropDuplicates()
    )

    print("Dimension : Warehouses Created")

    dim_products = (
        products
        .select(
            "product_id"
        )
        .dropDuplicates()
    )

    print("Dimension : Products Created")

    dim_carriers = (
        freight
        .select(
            col("freight_carrier").alias("carrier_id"),
            col("orig_port_cd").alias("origin_port"),
            col("dest_port_cd").alias("destination_port"),
            "tpt_day_cnt"
        )
        .dropDuplicates()
    )

    print("Dimension : Carriers Created")

    bridge_products_per_plant = (
    products
    .join(
        dim_warehouses,
        products.plant_code == dim_warehouses.plant_id,
        "inner"
    )
    .select(
        dim_warehouses.plant_id,
        products.product_id
    )
    .dropDuplicates()
)


    fact_orders = (
    final_df
    .select(
        col("order_id"),
        col("product_id"),
        col("plant_code").alias("plant_id"),
        col("carrier").alias("carrier_id"),
        col("destination_port"),
        col("unit_quantity"),
        col("weight").alias("unit_weight"),
        col("order_date"),
        col("tpt_day_cnt").alias("estimated_transit_days"),
        col("estimated_delivery_date")
    )
    .join(
        dim_warehouses.select("plant_id"),
        on="plant_id",
        how="inner"
    )
    .join(
        dim_carriers.select("carrier_id"),
        on="carrier_id",
        how="inner"
    )
    .dropDuplicates()
)
    fact_orders = fact_orders.select(
    "order_id",
    "product_id",
    "plant_id",
    "carrier_id",
    "destination_port",
    "unit_quantity",
    "unit_weight",
    "order_date",
    "estimated_transit_days",
    "estimated_delivery_date"
)


    print("Fact Orders Created")


    return (
        dim_warehouses,
        dim_products,
        dim_carriers,
        bridge_products_per_plant,
        fact_orders
    )