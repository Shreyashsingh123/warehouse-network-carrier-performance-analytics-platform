from pyspark.sql.functions import col, avg, count, rank, when, sum
from pyspark.sql.window import Window


class Querries:

    def _init_(self, spark):
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
