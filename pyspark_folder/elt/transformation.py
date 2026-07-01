from pyspark.sql.functions import (col,when,date_add,row_number)
from pyspark.sql.window import Window


def transform_data(orders,freight,capacity,cost,products,ports):

    orders = (
        orders
        .withColumnRenamed("Order ID", "order_id")
        .withColumnRenamed("Order Date", "order_date")
        .withColumnRenamed("Origin Port", "origin_port")
        .withColumnRenamed("Carrier", "carrier")
        .withColumnRenamed("Product ID", "product_id")
        .withColumnRenamed("Plant Code", "plant_code")
        .withColumnRenamed("Destination Port", "destination_port")
        .withColumnRenamed("Unit quantity", "unit_quantity")
        .withColumnRenamed("Weight", "weight")
    )

    freight = (
        freight
        .withColumnRenamed("Carrier", "freight_carrier")
        .withColumnRenamed("Carrier type", "carrier_type")
    )

    capacity = (
        capacity
        .withColumnRenamed("Plant ID", "plant_code")
        .withColumnRenamed("Daily Capacity ", "daily_capacity")
    )

    cost = (
        cost
        .withColumnRenamed("WH", "plant_code")
        .withColumnRenamed("Cost/unit", "unit_storage_cost")
    )

    products = (
        products
        .withColumnRenamed("Plant Code", "plant_code")
        .withColumnRenamed("Product ID", "product_id")
    )

    ports = (
        ports
        .withColumnRenamed("Plant Code", "plant_code")
        .withColumnRenamed("Port", "port")
    )

    print("Column Renaming Completed")
    orders_products = orders.join(
        products,
        on=["plant_code", "product_id"],
        how="left"
    )

    print("Join 1 Completed")
    orders_products_freight = orders_products.join(
        freight,
        (orders_products.carrier == freight.freight_carrier)
        &
        (orders_products.origin_port == freight.orig_port_cd)
        &
        (orders_products.destination_port == freight.dest_port_cd),
        "left"
    )
    print("Join 2 Completed")
    orders_products_capacity = orders_products_freight.join(
        capacity,
        on="plant_code",
        how="left"
    )
    print("Join 3 Completed")
    final_df = orders_products_capacity.join(
        cost,
        on="plant_code",
        how="left"
    )
    print("Join 4 Completed")
    window_spec = Window.partitionBy(
        "order_id"
    ).orderBy(
        "order_date"
    )
    final_df = (
        final_df
        .withColumn(
            "row_num",
            row_number().over(window_spec)
        )
        .filter(
            col("row_num") == 1
        )
        .drop(
            "row_num"
        )
    )
    print("Duplicates Removed")
    final_df = final_df.filter(
        col("unit_quantity") > 0
    )
    print("Quantity Validation Completed")
    final_df = final_df.withColumn(
        "weight",
        when(
            col("weight").isNull(),
            0
        ).otherwise(
            col("weight")
        )
    )
    print("Missing Weight Handled")

    final_df = final_df.withColumn(
        "estimated_delivery_date",
        date_add(
            col("order_date"),
            col("tpt_day_cnt")
        )
    )

    print("Estimated Delivery Date Created")

    return (
        final_df,
        capacity,
        cost,
        products,
        freight
    )