import os
import sys

from etl import main
from extraction import create_spark
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
sys.path.insert(0, PROJECT_ROOT)

from sql.analysis_queries.querry import Querries


def run():

    (
        final_df,
        warehouse_health,
        carrier_performance,
        order_routing_priority,
        dim_warehouses,
        dim_products,
        dim_carriers,
        fact_orders,
        freight
    ) = main()

    q=Querries(create_spark())
    print("ETL RUN COMPLETED")
    war=q.warehouses_above_90(warehouse_health)
    top=(q.top_10_carriers(final_df))
    order=(q.order_volume_by_warehouse(final_df))
    ranks=(q.rank_carriers(carrier_performance))
    warehouse=q.warehouse_overflow(final_df)
    running=q.running_orders(final_df)
    missing=q.missing_carriers(final_df, freight)
    cost=q.cost_by_tier(warehouse_health)
    ordesrs=(q.order_distribution(final_df))
    risk=q.warehouse_risk(final_df, warehouse_health)

    war.show()
    top.show()
    order.show()
    ranks.show()
    warehouse.show()
    running.show()
    missing.show()
    cost.show()
    ordesrs.show()
    risk.show()


    print("ALL QUERIES EXECUTED SUCCESSFULLY")
    


if __name__ == "__main__":
    run()