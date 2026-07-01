# S3 Configuration
S3_BUCKET = "warehouse-platform03"

RAW_PATH = f"s3a://{S3_BUCKET}/raw/"
CURATED_PATH = f"s3a://{S3_BUCKET}/curated/"


ORDER_LIST = RAW_PATH + "OrderList.csv"
FREIGHT_RATES = RAW_PATH + "FreightRates.csv"
WH_CAPACITIES = RAW_PATH + "WhCapacities.csv"
WH_COSTS = RAW_PATH + "WhCosts.csv"
PRODUCTS_PER_PLANT = RAW_PATH + "ProductsPerPlant.csv"
PLANT_PORTS = RAW_PATH + "PlantPorts.csv"

DB_HOST = "YOUR_RDS_ENDPOINT"
DB_PORT = "5432"
DB_NAME = "warehouse"
DB_USER = "postgres"
DB_PASSWORD = "password"