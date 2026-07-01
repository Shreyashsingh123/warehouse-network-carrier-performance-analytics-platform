import sys
import os

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
)
sys.path.append(PROJECT_ROOT)

from visualise import LogisticsVisualization

from pyspark_folder.elt.etl import main

final_df, warehouse_health, carrier_performance, order_routing_priority, dim_warehouses, dim_products, dim_carriers, fact_orders, freight = main()

# Create visualization object
viz = LogisticsVisualization(
    final_df,
    warehouse_health,
    carrier_performance,
    fact_orders
)

# Generate charts
viz.order_volume_trend()
viz.warehouse_utilization()
viz.carrier_transit_days()
viz.order_distribution_port()
viz.transit_days_histogram()
viz.carrier_port_heatmap()
viz.kpi_cards()