from db_connection import get_connection


def create_analytics_schema(cur):
    cur.execute("""
        CREATE SCHEMA IF NOT EXISTS analytics;
    """)


def create_dimension_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_warehouses(
            plant_id VARCHAR(20) PRIMARY KEY,
            daily_capacity INT,
            unit_storage_cost DECIMAL(10,4)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_products(
            product_id VARCHAR(20) PRIMARY KEY
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS dim_carriers(
            carrier_id VARCHAR(20) PRIMARY KEY,
            origin_port VARCHAR(20),
            destination_port VARCHAR(20),
            tpt_day_cnt INT
        );
    """)


def create_bridge_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS bridge_products_per_plant(
            plant_id VARCHAR(20),
            product_id VARCHAR(20),

            PRIMARY KEY (plant_id, product_id),

            FOREIGN KEY (plant_id)
                REFERENCES dim_warehouses(plant_id),

            FOREIGN KEY (product_id)
                REFERENCES dim_products(product_id)
        );
    """)


def create_fact_table(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS fact_orders(

            order_id BIGINT PRIMARY KEY,

            product_id VARCHAR(20),

            plant_id VARCHAR(20),

            carrier_id VARCHAR(20),

            destination_port VARCHAR(20),

            unit_quantity INT,

            unit_weight DECIMAL(10,2),

            order_date DATE,

            estimated_transit_days INT,

            estimated_delivery_date DATE,

            FOREIGN KEY(product_id)
                REFERENCES dim_products(product_id),

            FOREIGN KEY(plant_id)
                REFERENCES dim_warehouses(plant_id),

            FOREIGN KEY(carrier_id)
                REFERENCES dim_carriers(carrier_id)
        );
    """)


def create_analytics_tables(cur):
    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics.warehouse_health(

            health_id BIGSERIAL PRIMARY KEY,

            plant_id VARCHAR(20),

            report_date DATE,

            daily_order_count INT,

            utilization_pct DECIMAL(5,2),

            unit_storage_cost DECIMAL(10,4),

            FOREIGN KEY(plant_id)
                REFERENCES dim_warehouses(plant_id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics.carrier_performance(

            performance_id BIGSERIAL PRIMARY KEY,

            carrier_id VARCHAR(20),

            destination_port VARCHAR(20),

            avg_transit_days DECIMAL(6,2),

            order_count INT,

            rank_by_speed INT,

            FOREIGN KEY(carrier_id)
                REFERENCES dim_carriers(carrier_id)
        );
    """)

    cur.execute("""
        CREATE TABLE IF NOT EXISTS analytics.order_routing_priority(

            priority_id BIGSERIAL PRIMARY KEY,

            order_id BIGINT,

            utilization_pct DECIMAL(5,2),

            estimated_transit_days INT,

            risk_score DECIMAL(8,2),

            risk_rank INT,

            FOREIGN KEY(order_id)
                REFERENCES fact_orders(order_id)
        );
    """)


def main():
    conn = get_connection()
    cur = conn.cursor()

    create_analytics_schema(cur)
    create_dimension_tables(cur)
    create_bridge_table(cur)
    create_fact_table(cur)
    create_analytics_tables(cur)

    conn.commit()

    print("✅ RDS Schema Created Successfully")

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()