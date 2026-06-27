import psycopg2
from dotenv import load_dotenv
import os

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    database="postgres",
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASSWORD")
)

conn.autocommit = True

cur = conn.cursor()

db_name = os.getenv("DB_NAME")

cur.execute(f"SELECT 1 FROM pg_database WHERE datname='{db_name}'")

exists = cur.fetchone()

if exists:
    print(f"Database '{db_name}' already exists.")
else:
    cur.execute(f'CREATE DATABASE "{db_name}"')
    print(f"Database '{db_name}' created successfully.")

cur.close()
conn.close()