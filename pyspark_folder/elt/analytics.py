from pyspark.sql.functions import (
    col,
    count,
    avg,
    round,
    dense_rank,
    current_date
)

from pyspark.sql.window import Window


def create_analytics_tables(final_df, dim_carriers, fact_orders):

    # ==============================
    # WAREHOUSE HEALTH (FIXED)
    # ==============================
    warehouse_health = (
        final_df
        .groupBy("plant_code")
        .agg(
            count("order_id").alias("daily_order_count"),
            avg("daily_capacity").alias("daily_capacity"),
            avg("unit_storage_cost").alias("unit_storage_cost")
        )
        .withColumn(
            "utilization_pct",
            round(
                (col("daily_order_count") / col("daily_capacity")) * 100,
                2
            )
        )
        .withColumn("report_date", current_date())
        .select(
            "plant_code",
            "report_date",
            "daily_order_count",
            "utilization_pct",
            "unit_storage_cost"
        )
    )

    print("Warehouse Health Created")

    # ==============================
    # CARRIER PERFORMANCE
    # ==============================
    carrier_performance = (
        final_df
        .groupBy(
            col("carrier").alias("carrier_id"),
            "destination_port"
        )
        .agg(
            avg("tpt_day_cnt").alias("avg_transit_days"),
            count("order_id").alias("order_count")
        )
        .join(
            dim_carriers.select("carrier_id"),
            "carrier_id",
            "inner"
        )
    )

    rank_window = Window.orderBy(col("avg_transit_days"))

    carrier_performance = (
        carrier_performance
        .withColumn(
            "rank_by_speed",
            dense_rank().over(rank_window)
        )
        .select(
            "carrier_id",
            "destination_port",
            "avg_transit_days",
            "order_count",
            "rank_by_speed"
        )
    )

    print("Carrier Performance Created")

    # ==============================
    # ORDER ROUTING PRIORITY (FIXED JOIN)
    # ==============================
    order_routing_priority = (
        final_df
        .join(
            warehouse_health.select("plant_code", "utilization_pct"),
            on="plant_code",
            how="left"
        )
    )

    order_routing_priority = order_routing_priority.withColumn(
        "risk_score",
        round(
            (col("utilization_pct") * 0.6) +
            (col("tpt_day_cnt") * 0.4),
            2
        )
    )

    risk_window = Window.orderBy(col("risk_score").desc())

    order_routing_priority = order_routing_priority.withColumn(
        "risk_rank",
        dense_rank().over(risk_window)
    )

    order_routing_priority = (
        order_routing_priority
        .join(
            fact_orders.select("order_id"),
            "order_id",
            "inner"
        )
    )

    print("Order Routing Priority Created")

    return (
        warehouse_health,
        carrier_performance,
        order_routing_priority
    )