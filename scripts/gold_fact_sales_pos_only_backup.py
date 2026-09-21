import io
import os
from datetime import datetime

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

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)

# PostgreSQL connection
database_url = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

engine = create_engine(database_url)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_latest_silver_pos_file():
    response = s3.list_objects_v2(
        Bucket=SILVER_BUCKET,
        Prefix="pos/"
    )

    files = [
        obj for obj in response.get("Contents", [])
        if obj["Key"].endswith("pos_sales_clean.parquet")
    ]

    if not files:
        raise FileNotFoundError(
            "No cleaned Silver POS Parquet file was found."
        )

    return max(files, key=lambda obj: obj["LastModified"])["Key"]


def download_parquet(bucket, key):
    response = s3.get_object(Bucket=bucket, Key=key)

    return pd.read_parquet(
        io.BytesIO(response["Body"].read())
    )


# ============================================================
# MAIN PROCESSING
# ============================================================

print("=" * 60)
print("RetailLake Gold Layer - fact_sales_transaction")
print("=" * 60)

silver_key = get_latest_silver_pos_file()
print(f"\nSilver source: {silver_key}")

sales_df = download_parquet(SILVER_BUCKET, silver_key)

print(f"Silver sales rows: {len(sales_df)}")

# Standardize column names
sales_df.columns = [
    column.strip().lower().replace(" ", "_")
    for column in sales_df.columns
]

# Convert required fields
sales_df["transaction_date"] = pd.to_datetime(
    sales_df["transaction_date"],
    errors="coerce"
).dt.date

sales_df["quantity"] = pd.to_numeric(
    sales_df["quantity"],
    errors="coerce"
)

sales_df["unit_price"] = pd.to_numeric(
    sales_df["unit_price"],
    errors="coerce"
)

# Calculate net sales
sales_df["net_sales"] = (
    sales_df["quantity"] * sales_df["unit_price"]
)

# Add optional fields if they are missing
for column in ["payment_method", "sales_channel"]:
    if column not in sales_df.columns:
        sales_df[column] = "unknown"

sales_df["payment_method"] = (
    sales_df["payment_method"]
    .fillna("unknown")
    .astype(str)
    .str.strip()
    .str.lower()
)

sales_df["sales_channel"] = (
    sales_df["sales_channel"]
    .fillna("unknown")
    .astype(str)
    .str.strip()
    .str.lower()
)

# ============================================================
# LOAD GOLD DIMENSIONS
# ============================================================

print("\nLoading Gold dimensions...")

with engine.connect() as connection:
    dim_date = pd.read_sql(
        text("""
            SELECT date_key, full_date
            FROM gold.dim_date
        """),
        connection
    )

    dim_product = pd.read_sql(
        text("""
            SELECT product_key, product_id
            FROM gold.dim_product
        """),
        connection
    )

    dim_customer = pd.read_sql(
        text("""
            SELECT customer_key, customer_id
            FROM gold.dim_customer
        """),
        connection
    )

    dim_store = pd.read_sql(
        text("""
            SELECT store_key, store_id
            FROM gold.dim_store
        """),
        connection
    )

print(f"Products:  {len(dim_product)}")
print(f"Customers: {len(dim_customer)}")
print(f"Stores:    {len(dim_store)}")
print(f"Dates:     {len(dim_date)}")

# Standardize matching columns
sales_df["product_id"] = sales_df["product_id"].astype(str)
sales_df["customer_id"] = sales_df["customer_id"].astype(str)
sales_df["store_id"] = sales_df["store_id"].astype(str)

dim_product["product_id"] = dim_product["product_id"].astype(str)
dim_customer["customer_id"] = dim_customer["customer_id"].astype(str)
dim_store["store_id"] = dim_store["store_id"].astype(str)

dim_date["full_date"] = pd.to_datetime(
    dim_date["full_date"]
).dt.date

# ============================================================
# MATCH DIMENSION KEYS
# ============================================================

sales_df = sales_df.merge(
    dim_date,
    left_on="transaction_date",
    right_on="full_date",
    how="left"
)

sales_df = sales_df.merge(
    dim_product[["product_key", "product_id"]],
    on="product_id",
    how="left"
)

sales_df = sales_df.merge(
    dim_customer[["customer_key", "customer_id"]],
    on="customer_id",
    how="left"
)

sales_df = sales_df.merge(
    dim_store[["store_key", "store_id"]],
    on="store_id",
    how="left"
)

# Display unmatched records
print("\nForeign-key matching results:")
print(f"Unmatched products: {sales_df['product_key'].isna().sum()}")
print(f"Unmatched customers: {sales_df['customer_key'].isna().sum()}")
print(f"Unmatched stores: {sales_df['store_key'].isna().sum()}")
print(f"Unmatched dates: {sales_df['date_key'].isna().sum()}")

# Remove records with unmatched dimensions
before_count = len(sales_df)

sales_df = sales_df.dropna(
    subset=[
        "date_key",
        "product_key",
        "customer_key",
        "store_key"
    ]
).copy()

removed_count = before_count - len(sales_df)

print(f"Rows removed because of unmatched dimensions: {removed_count}")

# ============================================================
# PREPARE GOLD FACT TABLE
# ============================================================

gold_df = sales_df[
    [
        "transaction_id",
        "date_key",
        "product_key",
        "customer_key",
        "store_key",
        "quantity",
        "unit_price",
        "net_sales",
        "payment_method",
        "sales_channel"
    ]
].copy()

gold_df.insert(
    0,
    "sales_key",
    range(1, len(gold_df) + 1)
)

gold_df["date_key"] = gold_df["date_key"].astype(int)
gold_df["product_key"] = gold_df["product_key"].astype(int)
gold_df["customer_key"] = gold_df["customer_key"].astype(int)
gold_df["store_key"] = gold_df["store_key"].astype(int)

# ============================================================
# LOAD INTO POSTGRESQL
# ============================================================

print("\nLoading data into PostgreSQL...")

with engine.begin() as connection:
    connection.execute(
        text("DELETE FROM gold.fact_sales_transaction")
    )

gold_df.to_sql(
    "fact_sales_transaction",
    engine,
    schema="gold",
    if_exists="append",
    index=False,
    method="multi"
)

# Add primary key
 

print("\n" + "=" * 60)
print("GOLD fact_sales_transaction RESULTS")
print("=" * 60)
print(f"Silver rows:       {before_count}")
print(f"Gold rows loaded:  {len(gold_df)}")
print("\nGold table created/refreshed successfully:")
print("  gold.fact_sales_transaction")
print("\nGold fact_sales_transaction processing completed successfully.")
