from pyspark.sql import SparkSession
from .config import (
    ORDER_LIST,
    FREIGHT_RATES,
    WH_CAPACITIES,
    WH_COSTS,
    PRODUCTS_PER_PLANT,
    PLANT_PORTS
)


def create_spark():

    spark = (
        SparkSession.builder
        .appName("Warehouse Network Analytics")
        .config(
            "spark.jars.packages",
            "org.apache.hadoop:hadoop-aws:3.4.1,"
            "software.amazon.awssdk:bundle:2.29.52,"
            "software.amazon.awssdk:url-connection-client:2.29.52"
        )
        .config(
            "spark.hadoop.fs.s3a.endpoint",
            "s3.ap-south-1.amazonaws.com"
        )
        .config(
            "spark.hadoop.fs.s3a.path.style.access",
            "true"
        )
        .config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider"
        )
        .getOrCreate()
    )

    return spark


def read_data(spark):

    orders = spark.read.csv(
        ORDER_LIST,
        header=True,
        inferSchema=True
    )

    freight = spark.read.csv(
        FREIGHT_RATES,
        header=True,
        inferSchema=True
    )

    capacity = spark.read.csv(
        WH_CAPACITIES,
        header=True,
        inferSchema=True
    )

    cost = spark.read.csv(
        WH_COSTS,
        header=True,
        inferSchema=True
    )

    products = spark.read.csv(
        PRODUCTS_PER_PLANT,
        header=True,
        inferSchema=True
    )

    ports = spark.read.csv(
        PLANT_PORTS,
        header=True,
        inferSchema=True
    )

    print("All datasets loaded successfully.")

    return (
        orders,
        freight,
        capacity,
        cost,
        products,
        ports
    )


def validate_data(
        orders,
        freight,
        capacity,
        cost,
        products,
        ports
):

    print("=" * 50)
    print("ROW COUNTS")
    print("=" * 50)

    print("Orders :", orders.count())
    print("Freight :", freight.count())
    print("Capacity :", capacity.count())
    print("Cost :", cost.count())
    print("Products :", products.count())
    print("Ports :", ports.count())

    print("=" * 50)
    print("SCHEMA")
    print("=" * 50)

    orders.printSchema()
    freight.printSchema()
    capacity.printSchema()
    cost.printSchema()
    products.printSchema()
    ports.printSchema()

    return True