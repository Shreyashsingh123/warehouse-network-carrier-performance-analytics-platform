from pyspark.sql.functions import col, avg, count, rank, when, sum
from pyspark.sql.window import Window


class Querries:

    def __init__(self, spark):
        self.spark = spark

    def warehouses_above_90(self, warehouse_health):
        return warehouse_health.filter(col("utilization_pct") > 90)

    def top_10_carriers(self, final_df):
        return (
            final_df
            .groupBy("carrier")
            .agg(
                avg("tpt_day_cnt").alias("avg_transit_days"),
                count("order_id").alias("order_count")
            )
            .orderBy(col("avg_transit_days").asc())
            .limit(10)
        )
    def order_volume_by_warehouse(self, final_df):
        return (
            final_df
            .groupBy("plant_code")
            .agg(count("order_id").alias("total_orders"))
        )

    def rank_carriers(self, carrier_performance):
        window_spec = Window.partitionBy("destination_port") \
                            .orderBy("avg_transit_days")

        return carrier_performance.withColumn(
            "rank_by_speed",
            rank().over(window_spec)
        )
    
    def warehouse_overflow(self, final_df):
        return (
            final_df
            .groupBy("plant_code", "order_date")
            .count()
            .filter(col("count") > 3)
        )
    def running_orders(self, final_df):
        window_spec = Window.partitionBy("plant_code") \
                            .orderBy("order_date") \
                            .rowsBetween(Window.unboundedPreceding, 0)

        return final_df.withColumn(
            "running_order_count",
            sum("order_id").over(window_spec)
        )
    
    def missing_carriers(self, final_df, freight):
        return (
            final_df.select("carrier").distinct()
            .join(
                freight.select("freight_carrier").distinct(),
                final_df.carrier == freight.freight_carrier,
                "left_anti"
            )
        )
    def cost_by_tier(self, warehouse_health):
        return (
            warehouse_health
            .withColumn(
                "utilization_tier",
                when(col("utilization_pct") < 50, "LOW")
                .when(col("utilization_pct") < 80, "MEDIUM")
                .otherwise("HIGH")
            )
            .groupBy("utilization_tier")
            .agg(avg("unit_storage_cost").alias("avg_cost"))
        )
    def order_distribution(self, final_df):
        return (
            final_df
            .groupBy("destination_port")
            .agg(count("order_id").alias("total_orders"))
        )
    def warehouse_risk(self, final_df, warehouse_health):
        window_spec = Window.partitionBy("product_id") \
                            .orderBy(col("utilization_pct").desc())

        return (
            final_df
            .join(warehouse_health, "plant_code")
            .withColumn("risk_rank", rank().over(window_spec))
        )
