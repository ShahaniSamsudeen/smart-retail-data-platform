import os
from io import BytesIO

import boto3
import pandas as pd
import psycopg2
from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000"
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "retaillake_admin"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "retaillake_password"
)

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432"
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "retaillake"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "retaillake_user"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "change_me"
)

SILVER_BUCKET = "retaillake-silver"


# ============================================================
# CONNECT TO MINIO
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1"
)


# ============================================================
# FIND LATEST SILVER STORE FILE
# ============================================================

response = s3.list_objects_v2(
    Bucket=SILVER_BUCKET,
    Prefix="stores/"
)

objects = [
    obj
    for obj in response.get("Contents", [])
    if obj["Key"].endswith("stores_clean.parquet")
]

if not objects:
    raise FileNotFoundError(
        "No Silver store parquet file found."
    )

latest_object = max(
    objects,
    key=lambda x: x["LastModified"]
)

silver_key = latest_object["Key"]


# ============================================================
# DISPLAY INFORMATION
# ============================================================

print()
print("=" * 60)
print("RetailLake Gold Layer - dim_store")
print("=" * 60)

print(f"Silver source: {silver_key}")


# ============================================================
# DOWNLOAD SILVER DATA
# ============================================================

obj = s3.get_object(
    Bucket=SILVER_BUCKET,
    Key=silver_key
)

dim_store = pd.read_parquet(
    BytesIO(obj["Body"].read())
)

print(
    f"Silver store rows: {len(dim_store)}"
)


# ============================================================
# PREPARE DATA
# ============================================================

dim_store = dim_store.copy()

# Create surrogate key
dim_store["store_key"] = range(
    1,
    len(dim_store) + 1
)


# ============================================================
# CONNECT TO POSTGRESQL
# ============================================================

print()
print("Connecting to PostgreSQL...")

connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD
)

cursor = connection.cursor()

print("PostgreSQL connection successful!")


# ============================================================
# CREATE GOLD SCHEMA
# ============================================================

cursor.execute(
    """
    CREATE SCHEMA IF NOT EXISTS gold;
    """
)


# ============================================================
# CREATE DIM_STORE TABLE IF IT DOES NOT EXIST
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS gold.dim_store (

        store_key INTEGER PRIMARY KEY,

        store_id VARCHAR(50) NOT NULL UNIQUE,

        store_name VARCHAR(150),

        city VARCHAR(100),

        region VARCHAR(100)

    );
    """
)


# ============================================================
# INSERT / UPDATE STORES
# ============================================================

insert_query = """
    INSERT INTO gold.dim_store (
        store_key,
        store_id,
        store_name,
        city,
        region
    )
    VALUES (
        %s, %s, %s, %s, %s
    )
    ON CONFLICT (store_id)
    DO UPDATE SET

        store_name = EXCLUDED.store_name,

        city = EXCLUDED.city,

        region = EXCLUDED.region;
"""


processed = 0

for _, row in dim_store.iterrows():

    cursor.execute(
        insert_query,
        (
            int(row["store_key"]),
            str(row["store_id"]),
            row["store_name"],
            row["city"],
            row["region"],
        )
    )

    processed += 1


# ============================================================
# COMMIT
# ============================================================

connection.commit()


# ============================================================
# VERIFY TABLE
# ============================================================

cursor.execute(
    """
    SELECT COUNT(*)
    FROM gold.dim_store;
    """
)

row_count = cursor.fetchone()[0]

print()
print(f"Processed stores: {processed}")
print(f"Database rows: {row_count}")


# ============================================================
# DISPLAY COLUMNS
# ============================================================

cursor.execute(
    """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'gold'
      AND table_name = 'dim_store'
    ORDER BY ordinal_position;
    """
)

columns = [
    row[0]
    for row in cursor.fetchall()
]

print()
print("Gold dim_store columns:")
print(columns)


# ============================================================
# CLOSE CONNECTION
# ============================================================

cursor.close()
connection.close()


print()
print("=" * 60)
print("Gold table created/refreshed successfully:")
print("gold.dim_store")
print("=" * 60)