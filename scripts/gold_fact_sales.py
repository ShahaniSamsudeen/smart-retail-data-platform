import io
import os
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv(dotenv_path=Path.cwd() / ".env")

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

def get_latest_file(prefix, suffix):
    response = s3.list_objects_v2(
        Bucket=SILVER_BUCKET,
        Prefix=prefix
    )

    files = [
        obj for obj in response.get("Contents", [])
        if obj["Key"].endswith(suffix)
    ]

    if not files:
        raise FileNotFoundError(
            f"No file ending with {suffix} found under {prefix}"
        )

    return max(files, key=lambda obj: obj["LastModified"])["Key"]


def download_parquet(bucket, key):
    response = s3.get_object(
        Bucket=bucket,
        Key=key
    )

    return pd.read_parquet(
        io.BytesIO(response["Body"].read())
    )


# ============================================================
# LOAD POS DATA
# ============================================================

print("=" * 70)
print("RetailLake Gold Layer - Combined POS and E-commerce Sales")
print("=" * 70)

pos_key = get_latest_file(
    "pos/",
    "pos_sales_clean.parquet"
)

print(f"\nPOS Silver source: {pos_key}")

pos_df = download_parquet(
    SILVER_BUCKET,
    pos_key
)

pos_df.columns = [
    column.strip().lower().replace(" ", "_")
    for column in pos_df.columns
]

pos_df["transaction_id"] = pos_df["transaction_id"].astype(str)
pos_df["transaction_date"] = pd.to_datetime(
    pos_df["transaction_date"],
    errors="coerce"
).dt.date

pos_df["quantity"] = pd.to_numeric(
    pos_df["quantity"],
    errors="coerce"
)

pos_df["unit_price"] = pd.to_numeric(
    pos_df["unit_price"],
    errors="coerce"
)

pos_df["net_sales"] = (
    pos_df["quantity"] * pos_df["unit_price"]
)

pos_df["sales_channel"] = "physical store"

print(f"POS rows: {len(pos_df)}")


# ============================================================
# LOAD E-COMMERCE DATA
# ============================================================

ecommerce_key = get_latest_file(
    "ecommerce/",
    "ecommerce_orders_clean.parquet"
)

print(f"E-commerce Silver source: {ecommerce_key}")

ecommerce_df = download_parquet(
    SILVER_BUCKET,
    ecommerce_key
)

ecommerce_df.columns = [
    column.strip().lower().replace(" ", "_")
    for column in ecommerce_df.columns
]

# Convert e-commerce columns to the common fact-table format
ecommerce_df = ecommerce_df.rename(
    columns={
        "order_id": "transaction_id",
        "order_date": "transaction_date"
    }
)

ecommerce_df["transaction_id"] = ecommerce_df["transaction_id"].astype(str)

ecommerce_df["transaction_date"] = pd.to_datetime(
    ecommerce_df["transaction_date"],
    errors="coerce"
).dt.date

ecommerce_df["quantity"] = pd.to_numeric(
    ecommerce_df["quantity"],
    errors="coerce"
)

ecommerce_df["unit_price"] = pd.to_numeric(
    ecommerce_df["unit_price"],
    errors="coerce"
)

ecommerce_df["net_sales"] = (
    ecommerce_df["quantity"] * ecommerce_df["unit_price"]
)

ecommerce_df["payment_method"] = (
    ecommerce_df["payment_method"]
    .fillna("unknown")
    .astype(str)
    .str.strip()
    .str.lower()
)

ecommerce_df["sales_channel"] = "E-commerce"

# Map delivery regions to existing stores
region_to_store = {
    "Western": "ST001",
    "Central": "ST002",
    "Southern": "ST003",
    "Uva": "ST004",
    "North Western": "ST005",
    "Northern": "ST006",
    "Eastern": "ST007",
    "North Central": "ST008",
    "Sabaragamuwa": "ST009"
}

ecommerce_df["store_id"] = ecommerce_df["delivery_region"].map(
    region_to_store
)

print(f"E-commerce rows: {len(ecommerce_df)}")


# ============================================================
# STANDARDIZE POS COLUMNS
# ============================================================

for column in ["payment_method", "sales_channel"]:
    if column not in pos_df.columns:
        pos_df[column] = "unknown"

pos_df["payment_method"] = (
    pos_df["payment_method"]
    .fillna("unknown")
    .astype(str)
    .str.strip()
    .str.lower()
)

pos_df["store_id"] = pos_df["store_id"].astype(str)
pos_df["product_id"] = pos_df["product_id"].astype(str)
pos_df["customer_id"] = pos_df["customer_id"].astype(str)

ecommerce_df["product_id"] = ecommerce_df["product_id"].astype(str)
ecommerce_df["customer_id"] = ecommerce_df["customer_id"].astype(str)
ecommerce_df["store_id"] = ecommerce_df["store_id"].astype(str)

# Keep only the common columns
common_columns = [
    "transaction_id",
    "transaction_date",
    "product_id",
    "customer_id",
    "store_id",
    "quantity",
    "unit_price",
    "net_sales",
    "payment_method",
    "sales_channel"
]

pos_df = pos_df[common_columns]
ecommerce_df = ecommerce_df[common_columns]

# Combine both channels
sales_df = pd.concat(
    [pos_df, ecommerce_df],
    ignore_index=True
)

print(f"\nCombined sales rows: {len(sales_df)}")


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

dim_date["full_date"] = pd.to_datetime(
    dim_date["full_date"]
).dt.date

dim_product["product_id"] = dim_product["product_id"].astype(str)
dim_customer["customer_id"] = dim_customer["customer_id"].astype(str)
dim_store["store_id"] = dim_store["store_id"].astype(str)
# Replace invalid customer IDs with the controlled unknown member
valid_customer_ids = set(dim_customer["customer_id"])

invalid_customer_mask = ~sales_df["customer_id"].isin(
    valid_customer_ids
)

invalid_customer_count = invalid_customer_mask.sum()

sales_df.loc[
    invalid_customer_mask,
    "customer_id"
] = "UNKNOWN_CUSTOMER"

print(
    f"Customer IDs replaced with UNKNOWN_CUSTOMER: "
    f"{invalid_customer_count}"
)


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

print("\nForeign-key matching results:")
print(
    f"Unmatched products: "
    f"{sales_df['product_key'].isna().sum()}"
)
print(
    f"Unmatched customers: "
    f"{sales_df['customer_key'].isna().sum()}"
)
print(
    f"Unmatched stores: "
    f"{sales_df['store_key'].isna().sum()}"
)
print(
    f"Unmatched dates: "
    f"{sales_df['date_key'].isna().sum()}"
)

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

print(
    f"Rows removed because of unmatched dimensions: "
    f"{removed_count}"
)


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

for column in [
    "date_key",
    "product_key",
    "customer_key",
    "store_key"
]:
    gold_df[column] = gold_df[column].astype(int)


# ============================================================
# LOAD INTO POSTGRESQL
# ============================================================

print("\nRefreshing Gold fact table...")

with engine.begin() as connection:
    connection.execute(
        text("DELETE FROM gold.fact_sales_transaction")
    )

    gold_df.to_sql(
        "fact_sales_transaction",
        connection,
        schema="gold",
        if_exists="append",
        index=False,
        method="multi"
    )

print("\n" + "=" * 70)
print("GOLD FACT SALES RESULTS")
print("=" * 70)
print(f"POS rows:                  {len(pos_df)}")
print(f"E-commerce rows:           {len(ecommerce_df)}")
print(f"Combined source rows:      {before_count}")
print(f"Rows removed:              {removed_count}")
print(f"Gold rows loaded:          {len(gold_df)}")

print("\nSales-channel summary:")
print(
    gold_df.groupby("sales_channel")["net_sales"]
    .agg(["count", "sum"])
)

print("\nGold table refreshed successfully:")
print("  gold.fact_sales_transaction")
print("  gold.vw_sales_dashboard")

print("\nProcessing completed successfully.")