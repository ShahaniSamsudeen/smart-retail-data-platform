
import io
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "retaillake_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "retaillake_password")

SILVER_BUCKET = "retaillake-silver"

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = os.getenv("POSTGRES_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "retaillake")
POSTGRES_USER = os.getenv("POSTGRES_USER", "retaillake_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

engine = create_engine(DATABASE_URL)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_latest_silver_file():
    """Find the latest clean DummyJSON Silver Parquet file."""

    response = s3.list_objects_v2(
        Bucket=SILVER_BUCKET,
        Prefix="api/dummyjson/"
    )

    files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(
            "dummyjson_products_clean.parquet"
        )
    ]

    if not files:
        raise FileNotFoundError(
            "No clean DummyJSON Silver Parquet file was found."
        )

    return sorted(files)[-1]


# ============================================================
# MAIN PROCESSING
# ============================================================

print("=" * 60)
print("RetailLake Gold Layer - DummyJSON API")
print("=" * 60)

silver_key = get_latest_silver_file()

print(f"\nSilver API source: {silver_key}")

response = s3.get_object(
    Bucket=SILVER_BUCKET,
    Key=silver_key
)

df = pd.read_parquet(
    io.BytesIO(response["Body"].read())
)

print(f"Silver API rows: {len(df)}")

# Remove audit columns that are not needed in the Gold table
audit_columns = [
    "_source",
    "_processed_at",
]

df = df.drop(
    columns=[
        column
        for column in audit_columns
        if column in df.columns
    ]
)

# Add Gold-layer audit information
df["loaded_at"] = datetime.now(
    timezone.utc
).isoformat()

# Ensure PostgreSQL-compatible column names
df.columns = [
    column.strip().lower().replace(" ", "_")
    for column in df.columns
]

# Create schema and table
with engine.begin() as connection:

    connection.execute(
        text("CREATE SCHEMA IF NOT EXISTS gold")
    )

    connection.execute(
        text("""
            CREATE TABLE IF NOT EXISTS gold.dim_product_dummyjson (
                id INTEGER PRIMARY KEY,
                title TEXT,
                description TEXT,
                category TEXT,
                price NUMERIC(12, 2),
                discount_percentage NUMERIC(8, 2),
                rating NUMERIC(5, 2),
                stock INTEGER,
                brand TEXT,
                sku TEXT,
                loaded_at TIMESTAMPTZ
            )
        """)
    )

    # Refresh the table while preserving its structure
    connection.execute(
        text(
            "TRUNCATE TABLE gold.dim_product_dummyjson"
        )
    )

# Load data into PostgreSQL
df.to_sql(
    "dim_product_dummyjson",
    engine,
    schema="gold",
    if_exists="append",
    index=False,
    method="multi"
)

# Validate loaded row count
with engine.connect() as connection:
    result = connection.execute(
        text(
            "SELECT COUNT(*) "
            "FROM gold.dim_product_dummyjson"
        )
    )

    gold_count = result.scalar()

print("\n" + "=" * 60)
print("DUMMYJSON GOLD RESULTS")
print("=" * 60)
print(f"Silver rows: {len(df)}")
print(f"Gold rows:   {gold_count}")
print("Gold table:  gold.dim_product_dummyjson")

print("\nDummyJSON Gold processing completed successfully.")