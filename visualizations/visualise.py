from pyspark.sql.functions import col, count

import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

from pyspark_folder.elt.config import CURATED_PATH

sns.set_style("whitegrid")


class LogisticsVisualization:

    def __init__(
            
        self,
        final_df,
        warehouse_health,
        carrier_performance,
        fact_orders
    ):
        
        self.final_df = final_df
        self.warehouse_health = warehouse_health.toPandas()
        self.carrier_performance = carrier_performance.toPandas()
        self.fact_orders = fact_orders.toPandas()
        
   

    
    def order_volume_trend(self,plant_code=None):

        trend = (
            self.final_df.groupby(["order_date", "plant_code"]).agg(count("order_id").alias("count")).orderBy("order_date")
        )
        print(
        self.final_df.select("order_date")
        .distinct()
        .orderBy("order_date")
        .show(20, False)
)
        pdf=trend.toPandas()
        if plant_code:
            pdf = pdf[pdf["plant_code"] == plant_code]
        
        pdf=pdf.sort_values("order_date")
        plt.figure(figsize=(10, 6))
        if plant_code:
            plt.plot(pdf["order_date"], pdf["count"], marker="o", label=f"Plant {plant_code}") 
            plt.title("Daily Order Volume Trend By Warehouse")
        else:
            for plant in pdf["plant_code"].unique():
                plant_data = pdf[pdf["plant_code"] == plant]
                plt.plot(plant_data["order_date"], plant_data["count"], marker="o", label=f"Plant {plant}")
            plt.title("Daily Order Volume Trend By Warehouse")
        plt.xlabel("Order Date")
        plt.ylabel("Order Count")
        plt.xticks(rotation=45)
        plt.legend()
        plt.tight_layout()
        plt.show()


    # 2. A. Warehouse Capacity Utilization
    def warehouse_utilization(self):

        plt.figure(figsize=(10, 5))

        sns.barplot(
            data=self.warehouse_health,
            x="plant_code",
            y="utilization_pct"
        )

        plt.title("Warehouse Capacity Utilization (%)")
        plt.xlabel("Warehouse")
        plt.ylabel("Utilization %")

        plt.tight_layout()
        plt.show()
    
    # 2. B. Average Transit Days By Carrier
    def carrier_transit_days(self):

        plt.figure(figsize=(10, 5))

        sns.barplot(
            data=self.carrier_performance,
            x="carrier_id",
            y="avg_transit_days"
        )

        plt.title("Average Transit Days By Carrier")
        plt.xlabel("Carrier")
        plt.ylabel("Average Transit Days")

        plt.xticks(rotation=45)
        plt.tight_layout()
        plt.show()

    # 3. Order Distribution By Destination Port
    def order_distribution_port(self):

        port_data = (
            self.fact_orders
            .groupby("destination_port")
            .size()
            .reset_index(name="orders")
        )

        plt.figure(figsize=(8, 8))

        plt.pie(
            port_data["orders"],
            labels=port_data["destination_port"],
            autopct="%1.1f%%"
        )

        plt.title("Order Distribution By Destination Port")
        plt.show()

    # 5. Transit Days Histogram
 
    def transit_days_histogram(self):

        plt.figure(figsize=(10, 5))

        sns.histplot(
            self.fact_orders["estimated_transit_days"],
            bins=15,
            kde=True
        )

        plt.title("Distribution Of Transit Days")
        plt.xlabel("Transit Days")

        plt.tight_layout()
        plt.show()

    # 6. Carrier vs Port Heatmap
    
    def carrier_port_heatmap(self):

        heatmap_df = (
            self.carrier_performance
            .pivot_table(
                values="avg_transit_days",
                index="carrier_id",
                columns="destination_port",
                aggfunc="mean"
            )
        )

        plt.figure(figsize=(12, 6))

        sns.heatmap(
            heatmap_df,
            annot=True,
            cmap="YlGnBu"
        )

        plt.title("Carrier vs Destination Port Transit Days")

        plt.tight_layout()
        plt.show()

     # 7. KPI Dashboard
    def kpi_cards(self):

        warehouses_above_90 = (
            self.warehouse_health[
                self.warehouse_health["utilization_pct"] > 90
            ]["plant_code"].nunique()
        )
        fastest_carrier = (
            self.carrier_performance
            .sort_values("avg_transit_days")
            .iloc[0]["carrier_id"]
        )
        at_risk_orders = (
            self.fact_orders[
                self.fact_orders["estimated_transit_days"] > 7
            ].shape[0]
        )
        print("\n" + "=" * 50)
        print("KPI DASHBOARD")
        print("=" * 50)
        print(f"Warehouses Above 90% Capacity : {warehouses_above_90}")
        print(f"Fastest Carrier               : {fastest_carrier}")
        print(f"At-Risk Orders                : {at_risk_orders}")
        print("=" * 50)




    