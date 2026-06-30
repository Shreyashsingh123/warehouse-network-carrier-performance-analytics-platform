from extraction import create_spark, read_data, validate_data
from transformation import transform_data
from dimensions import create_dimension_tables
from analytics import create_analytics_tables
from loading import write_to_s3
from db_loader import load_to_postgres

def main():
    spark = create_spark()
    (
        orders,
        freight,
        capacity,
        cost,
        products,
        ports
    ) = read_data(spark)

    validate_data(
        orders,
        freight,
        capacity,
        cost,
        products,
        ports
    )
    (
        final_df,
        capacity,
        cost,
        products,
        freight
    ) = transform_data(
        orders,
        freight,
        capacity,
        cost,
        products,
        ports
    )
    (
        dim_warehouses,
        dim_products,
        dim_carriers,
        bridge_products_per_plant,
        fact_orders
    ) = create_dimension_tables(
        final_df,
        capacity,
        cost,
        products,
        freight
    )

    (
        warehouse_health,
        carrier_performance,
        order_routing_priority
    ) = create_analytics_tables(
        final_df,dim_carriers,fact_orders
    )

    write_to_s3(
        dim_warehouses,
        dim_products,
        dim_carriers,
        bridge_products_per_plant,
        fact_orders,
        warehouse_health,
        carrier_performance, 
        order_routing_priority
    )
    order_routing_priority.filter(
    order_routing_priority.order_id == 1447138895).show()
    load_to_postgres(
    dim_warehouses,
    dim_products,
    dim_carriers,
    bridge_products_per_plant,
    fact_orders,
    warehouse_health,
    carrier_performance,
    order_routing_priority
)


    print("=" * 60)
    print("Warehouse ETL Pipeline Completed Successfully")
    print("=" * 60)


if __name__ == "__main__":
    main()