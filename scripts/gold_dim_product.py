import io
import os

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

BRONZE_BUCKET = "retaillake-bronze"
SILVER_BUCKET = "retaillake-silver"

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = int(
    os.getenv(
        "POSTGRES_PORT",
        "5432"
    )
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


# ============================================================
# CONFIGURATION
# ============================================================

SILVER_PREFIX = "products/"

print()
print("=" * 60)
print("RetailLake Gold Layer - dim_product")
print("=" * 60)


# ============================================================
# MINIO CONNECTION
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


# ============================================================
# FIND LATEST SILVER PRODUCT FILE
# ============================================================

response = s3.list_objects_v2(
    Bucket=SILVER_BUCKET,
    Prefix=SILVER_PREFIX
)

objects = response.get(
    "Contents",
    []
)

objects = [
    obj
    for obj in objects
    if obj["Key"].endswith(
        "products_clean.parquet"
    )
]

if not objects:
    raise FileNotFoundError(
        "No Silver product Parquet file found."
    )

latest = max(
    objects,
    key=lambda obj: obj["LastModified"]
)

silver_key = latest["Key"]

print(
    f"Silver source: {silver_key}"
)


# ============================================================
# DOWNLOAD SILVER PRODUCT FILE
# ============================================================

response = s3.get_object(
    Bucket=SILVER_BUCKET,
    Key=silver_key
)

df = pd.read_parquet(
    io.BytesIO(
        response["Body"].read()
    )
)

print(
    f"Silver product rows: {len(df)}"
)


# ============================================================
# POSTGRESQL CONNECTION
# ============================================================

print()
print("Connecting to PostgreSQL...")

connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
)

cursor = connection.cursor()

print(
    "PostgreSQL connection successful!"
)


# ============================================================
# CREATE GOLD SCHEMA
# ============================================================

cursor.execute(
    """
    CREATE SCHEMA IF NOT EXISTS gold;
    """
)

connection.commit()


# ============================================================
# CREATE DIM_PRODUCT IF IT DOES NOT EXIST
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS gold.dim_product (

        product_key INTEGER PRIMARY KEY,

        product_id VARCHAR(50) NOT NULL UNIQUE,

        product_name VARCHAR(255) NOT NULL,

        category VARCHAR(100) NOT NULL,

        brand VARCHAR(100),

        unit_cost NUMERIC(12, 2),

        selling_price NUMERIC(12, 2),

        supplier_id VARCHAR(50)

    );
    """
)

connection.commit()


# ============================================================
# PREPARE PRODUCT DATA
# ============================================================

df = df.copy()

df = df.reset_index(
    drop=True
)

df["product_key"] = (
    df.index + 1
)


# ============================================================
# INSERT / UPDATE PRODUCTS
# ============================================================

insert_query = """
    INSERT INTO gold.dim_product (

        product_key,
        product_id,
        product_name,
        category,
        brand,
        unit_cost,
        selling_price,
        supplier_id

    )
    VALUES (
        %s, %s, %s, %s, %s,
        %s, %s, %s
    )

    ON CONFLICT (product_id)
    DO UPDATE SET

        product_name = EXCLUDED.product_name,

        category = EXCLUDED.category,

        brand = EXCLUDED.brand,

        unit_cost = EXCLUDED.unit_cost,

        selling_price = EXCLUDED.selling_price,

        supplier_id = EXCLUDED.supplier_id;
"""


for _, row in df.iterrows():

    cursor.execute(
        insert_query,
        (
            int(row["product_key"]),
            str(row["product_id"]),
            row["product_name"],
            row["category"],
            row.get("brand"),
            row.get("unit_cost"),
            row.get("selling_price"),
            row.get("supplier_id"),
        )
    )


connection.commit()


# ============================================================
# VALIDATE ROW COUNT
# ============================================================

cursor.execute(
    """
    SELECT COUNT(*)
    FROM gold.dim_product;
    """
)

row_count = cursor.fetchone()[0]


# ============================================================
# CLOSE CONNECTION
# ============================================================

cursor.close()
connection.close()


# ============================================================
# RESULTS
# ============================================================

print()
print("=" * 60)
print("DIM_PRODUCT PROCESSING RESULTS")
print("=" * 60)

print(
    f"Silver rows:      {len(df)}"
)

print(
    f"Database rows:    {row_count}"
)

print()
print(
    "Gold table created/refreshed successfully:"
)

print(
    "  gold.dim_product"
)

print()
print(
    "Gold dim_product processing completed successfully."
)