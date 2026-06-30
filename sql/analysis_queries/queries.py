from pyspark.sql.functions import col, avg, count, rank, when, sum
from pyspark.sql.window import Window


class Querries:

    def _init_(self, spark):
        self.spark = spark

    def warehouses_above_90(self, warehouse_health):
        return warehouse_health.filter(col("utilization_pct") > 90)