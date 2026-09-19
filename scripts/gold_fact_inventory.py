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

SILVER_BUCKET = "retaillake-silver"

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
# FIND LATEST SILVER FILE
# ============================================================

def get_latest_file(prefix, filename):

    response = s3.list_objects_v2(
        Bucket=SILVER_BUCKET,
        Prefix=prefix
    )

    objects = response.get("Contents", [])

    objects = [
        obj
        for obj in objects
        if obj["Key"].endswith(filename)
    ]

    if not objects:
        raise FileNotFoundError(
            f"No Silver file found: {prefix}{filename}"
        )

    return max(
        objects,
        key=lambda obj: obj["LastModified"]
    )["Key"]


# ============================================================
# DOWNLOAD PARQUET
# ============================================================

def download_parquet(object_key):

    response = s3.get_object(
        Bucket=SILVER_BUCKET,
        Key=object_key
    )

    return pd.read_parquet(
        io.BytesIO(
            response["Body"].read()
        )
    )


# ============================================================
# POSTGRESQL CONNECTION
# ============================================================

def get_postgres_connection():

    return psycopg2.connect(
        host=POSTGRES_HOST,
        port=POSTGRES_PORT,
        dbname=POSTGRES_DB,
        user=POSTGRES_USER,
        password=POSTGRES_PASSWORD
    )


# ============================================================
# MAIN
# ============================================================

print("\n" + "=" * 60)
print("RetailLake Gold Layer - fact_inventory_snapshot")
print("=" * 60)


# ============================================================
# LOAD SILVER INVENTORY
# ============================================================

inventory_key = get_latest_file(
    "inventory/",
    "inventory_clean.parquet"
)

print(
    f"\nInventory source: "
    f"{inventory_key}"
)

inventory_df = download_parquet(
    inventory_key
)

print(
    f"Inventory clean rows: "
    f"{len(inventory_df)}"
)


# ============================================================
# CONNECT TO POSTGRESQL
# ============================================================

print("\nConnecting to PostgreSQL...")

conn = get_postgres_connection()

cursor = conn.cursor()

print("PostgreSQL connection successful!")


# ============================================================
# LOAD GOLD DIMENSIONS
# ============================================================

cursor.execute(
    """
    SELECT product_key, product_id
    FROM gold.dim_product;
    """
)

product_rows = cursor.fetchall()

product_lookup = {
    product_id: product_key
    for product_key, product_id
    in product_rows
}


cursor.execute(
    """
    SELECT store_key, store_id
    FROM gold.dim_store;
    """
)

store_rows = cursor.fetchall()

store_lookup = {
    store_id: store_key
    for store_key, store_id
    in store_rows
}


cursor.execute(
    """
    SELECT date_key, full_date
    FROM gold.dim_date;
    """
)

date_rows = cursor.fetchall()

date_lookup = {
    full_date: date_key
    for date_key, full_date
    in date_rows
}


print("\nGold dimensions loaded:")
print(f"  Products: {len(product_lookup)}")
print(f"  Stores:   {len(store_lookup)}")
print(f"  Dates:    {len(date_lookup)}")


# ============================================================
# CREATE FACT DATA
# ============================================================

fact_inventory = pd.DataFrame()

fact_inventory["snapshot_date"] = pd.to_datetime(
    inventory_df["snapshot_date"]
).dt.date

fact_inventory["date_key"] = (
    fact_inventory["snapshot_date"]
    .map(date_lookup)
)

fact_inventory["product_key"] = (
    inventory_df["product_id"]
    .astype(str)
    .map(product_lookup)
)

fact_inventory["store_key"] = (
    inventory_df["store_id"]
    .astype(str)
    .map(store_lookup)
)

fact_inventory["stock_quantity"] = (
    pd.to_numeric(
        inventory_df["stock_quantity"],
        errors="coerce"
    )
)

fact_inventory["reorder_level"] = (
    pd.to_numeric(
        inventory_df["reorder_level"],
        errors="coerce"
    )
)

fact_inventory["stock_status"] = (
    inventory_df["stock_status"]
)


# ============================================================
# CHECK FOREIGN KEY MATCHING
# ============================================================

unmatched_products = (
    fact_inventory["product_key"]
    .isna()
    .sum()
)

unmatched_stores = (
    fact_inventory["store_key"]
    .isna()
    .sum()
)

unmatched_dates = (
    fact_inventory["date_key"]
    .isna()
    .sum()
)


print("\nForeign-key matching results:")

print(
    f"  Unmatched products: "
    f"{unmatched_products}"
)

print(
    f"  Unmatched stores:   "
    f"{unmatched_stores}"
)

print(
    f"  Unmatched dates:    "
    f"{unmatched_dates}"
)


# ============================================================
# REMOVE UNMATCHED RECORDS
# ============================================================

before_filter = len(fact_inventory)

fact_inventory = fact_inventory[
    fact_inventory["product_key"].notna()
    &
    fact_inventory["store_key"].notna()
    &
    fact_inventory["date_key"].notna()
].copy()

after_filter = len(fact_inventory)


print(
    f"\nRows removed because of "
    f"unmatched dimensions: "
    f"{before_filter - after_filter}"
)


# ============================================================
# CONVERT KEYS TO INTEGER
# ============================================================

fact_inventory["date_key"] = (
    fact_inventory["date_key"].astype(int)
)

fact_inventory["product_key"] = (
    fact_inventory["product_key"].astype(int)
)

fact_inventory["store_key"] = (
    fact_inventory["store_key"].astype(int)
)


# ============================================================
# CREATE GOLD FACT TABLE
# ============================================================

cursor.execute(
    """
    DROP TABLE IF EXISTS gold.fact_inventory_snapshot;

    CREATE TABLE gold.fact_inventory_snapshot (

        inventory_key BIGSERIAL PRIMARY KEY,

        date_key INTEGER NOT NULL,

        product_key INTEGER NOT NULL,

        store_key INTEGER NOT NULL,

        stock_quantity NUMERIC(12, 2) NOT NULL,

        reorder_level NUMERIC(12, 2) NOT NULL,

        stock_status VARCHAR(50),

        CONSTRAINT fk_inventory_date
            FOREIGN KEY (date_key)
            REFERENCES gold.dim_date(date_key),

        CONSTRAINT fk_inventory_product
            FOREIGN KEY (product_key)
            REFERENCES gold.dim_product(product_key),

        CONSTRAINT fk_inventory_store
            FOREIGN KEY (store_key)
            REFERENCES gold.dim_store(store_key)

    );
    """
)


# ============================================================
# INSERT INVENTORY FACT DATA
# ============================================================

insert_query = """
    INSERT INTO gold.fact_inventory_snapshot (
        date_key,
        product_key,
        store_key,
        stock_quantity,
        reorder_level,
        stock_status
    )
    VALUES (
        %s, %s, %s, %s, %s, %s
    )
"""


for _, row in fact_inventory.iterrows():

    cursor.execute(
        insert_query,
        (
            int(row["date_key"]),
            int(row["product_key"]),
            int(row["store_key"]),
            float(row["stock_quantity"]),
            float(row["reorder_level"]),
            row["stock_status"]
        )
    )


# ============================================================
# COMMIT
# ============================================================

conn.commit()


# ============================================================
# VERIFY TABLE
# ============================================================

cursor.execute(
    """
    SELECT COUNT(*)
    FROM gold.fact_inventory_snapshot;
    """
)

row_count = cursor.fetchone()[0]


cursor.execute(
    """
    SELECT
        inventory_key,
        date_key,
        product_key,
        store_key,
        stock_quantity,
        reorder_level,
        stock_status
    FROM gold.fact_inventory_snapshot
    ORDER BY inventory_key
    LIMIT 5;
    """
)

sample_rows = cursor.fetchall()


# ============================================================
# STOCK SUMMARY
# ============================================================

cursor.execute(
    """
    SELECT
        stock_status,
        COUNT(*)
    FROM gold.fact_inventory_snapshot
    GROUP BY stock_status
    ORDER BY stock_status;
    """
)

stock_summary = cursor.fetchall()


# ============================================================
# CLOSE CONNECTION
# ============================================================

cursor.close()
conn.close()


# ============================================================
# DISPLAY RESULTS
# ============================================================

print("\n" + "=" * 60)
print("GOLD fact_inventory_snapshot RESULTS")
print("=" * 60)

print(
    f"Inventory rows loaded into Gold: "
    f"{row_count}"
)

print("\nStock status summary:")

for status, count in stock_summary:
    print(
        f"  {status}: {count}"
    )

print("\nSample records:")

for row in sample_rows:
    print(row)


print("\nGold table created successfully:")
print("  gold.fact_inventory_snapshot")

print(
    "\nGold fact_inventory_snapshot "
    "processing completed successfully."
)