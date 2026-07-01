from .config import CURATED_PATH


def write_to_s3(
    dim_warehouses,
    dim_products,
    dim_carriers,
    bridge_products_per_plant,
    fact_orders,
    warehouse_health,
    carrier_performance,
    order_routing_priority
):

    print("=" * 50)
    print("Writing Curated Data To S3")
    print("=" * 50)

    dim_warehouses.write.mode("overwrite").parquet(
        CURATED_PATH + "dim_warehouses"
    )

    dim_products.write.mode("overwrite").parquet(
        CURATED_PATH + "dim_products"
    )

    dim_carriers.write.mode("overwrite").parquet(
        CURATED_PATH + "dim_carriers"
    )

    bridge_products_per_plant.write.mode("overwrite").parquet(
        CURATED_PATH + "bridge_products_per_plant"
    )

    fact_orders.write.mode("overwrite").parquet(
        CURATED_PATH + "fact_orders"
    )

    warehouse_health.write.mode("overwrite").parquet(
        CURATED_PATH + "warehouse_health"
    )

    carrier_performance.write.mode("overwrite").parquet(
        CURATED_PATH + "carrier_performance"
    )

    order_routing_priority.write.mode("overwrite").parquet(
        CURATED_PATH + "order_routing_priority"
    )

    print("All Curated Files Successfully Written To S3")