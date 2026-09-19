import os
import io
import json
from datetime import datetime

import boto3
import pandas as pd
from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000"
)

MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

RAW_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "retaillake-raw"
)

BRONZE_BUCKET = "retaillake-bronze"


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
# HELPER FUNCTIONS
# ============================================================

def get_latest_object(prefix, extension=None):
    """
    Find the latest file under a Raw-layer prefix.
    """

    response = s3.list_objects_v2(
        Bucket=RAW_BUCKET,
        Prefix=prefix
    )

    objects = response.get("Contents", [])

    if extension:
        objects = [
            obj
            for obj in objects
            if obj["Key"].lower().endswith(extension)
        ]

    if not objects:
        raise FileNotFoundError(
            f"No files found in Raw bucket under: {prefix}"
        )

    latest = max(
        objects,
        key=lambda obj: obj["LastModified"]
    )

    return latest["Key"]


def download_object(object_key):
    """
    Download an object from MinIO into memory.
    """

    response = s3.get_object(
        Bucket=RAW_BUCKET,
        Key=object_key
    )

    return response["Body"].read()


def upload_parquet(df, object_key):
    """
    Convert DataFrame to Parquet and upload to Bronze.
    """

    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow"
    )

    buffer.seek(0)

    s3.put_object(
        Bucket=BRONZE_BUCKET,
        Key=object_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )


def add_audit_columns(df, source_name):
    """
    Add Bronze audit metadata.
    """

    df["_source"] = source_name
    df["_ingested_at"] = datetime.now().isoformat()

    return df


def standardize_date_column(df, column_name):
    """
    Convert a date/timestamp column into a proper datetime type.

    Bronze performs typing only.
    Data cleaning and quarantine will happen later in Silver.
    """

    if column_name in df.columns:
        df[column_name] = pd.to_datetime(
            df[column_name],
            errors="coerce",
            format="mixed"
        )

    return df


def standardize_numeric_column(df, column_name):
    """
    Convert numeric fields to numeric data types.
    """

    if column_name in df.columns:
        df[column_name] = pd.to_numeric(
            df[column_name],
            errors="coerce"
        )

    return df


# ============================================================
# CREATE BRONZE BUCKET IF NEEDED
# ============================================================

print("\n" + "=" * 60)
print("RetailLake Bronze Layer - All Sources Including API")
print("=" * 60)

try:
    s3.head_bucket(
        Bucket=BRONZE_BUCKET
    )

    print(
        f"Bronze bucket '{BRONZE_BUCKET}' already exists."
    )

except Exception:
    s3.create_bucket(
        Bucket=BRONZE_BUCKET
    )

    print(
        f"Created Bronze bucket '{BRONZE_BUCKET}'."
    )


# ============================================================
# PROCESS DATE
# ============================================================

processing_date = datetime.now()

year = processing_date.strftime("%Y")
month = processing_date.strftime("%m")
day = processing_date.strftime("%d")


# ============================================================
# 1. POS SALES
# ============================================================

print("\n[1/9] Processing POS Sales...")

pos_key = get_latest_object(
    "pos/",
    ".csv"
)

print(f"Raw source: {pos_key}")

pos_bytes = download_object(pos_key)

pos_df = pd.read_csv(
    io.BytesIO(pos_bytes)
)

print(f"Rows: {len(pos_df)}")

pos_df = standardize_date_column(
    pos_df,
    "transaction_date"
)

pos_df = standardize_numeric_column(
    pos_df,
    "quantity"
)

pos_df = standardize_numeric_column(
    pos_df,
    "unit_price"
)

pos_df["sales_amount"] = (
    pos_df["quantity"] *
    pos_df["unit_price"]
)

pos_df = add_audit_columns(
    pos_df,
    "POS"
)

upload_parquet(
    pos_df,
    f"pos/{year}/{month}/{day}/pos_sales.parquet"
)

print("POS Bronze completed.")


# ============================================================
# 2. E-COMMERCE ORDERS
# ============================================================

print("\n[2/9] Processing E-commerce Orders...")

ecommerce_key = get_latest_object(
    "ecommerce/",
    ".json"
)

print(f"Raw source: {ecommerce_key}")

ecommerce_bytes = download_object(
    ecommerce_key
)

ecommerce_df = pd.read_json(
    io.BytesIO(ecommerce_bytes)
)

print(f"Rows: {len(ecommerce_df)}")

ecommerce_df = standardize_date_column(
    ecommerce_df,
    "order_date"
)

ecommerce_df = standardize_numeric_column(
    ecommerce_df,
    "quantity"
)

ecommerce_df = standardize_numeric_column(
    ecommerce_df,
    "unit_price"
)

ecommerce_df["sales_amount"] = (
    ecommerce_df["quantity"] *
    ecommerce_df["unit_price"]
)

ecommerce_df = add_audit_columns(
    ecommerce_df,
    "E_COMMERCE"
)

upload_parquet(
    ecommerce_df,
    f"ecommerce/{year}/{month}/{day}/ecommerce_orders.parquet"
)

print("E-commerce Bronze completed.")


# ============================================================
# 3. CUSTOMERS / CRM
# ============================================================

print("\n[3/9] Processing CRM Customers...")

customer_key = get_latest_object(
    "crm/",
    ".csv"
)

print(f"Raw source: {customer_key}")

customer_bytes = download_object(
    customer_key
)

customer_df = pd.read_csv(
    io.BytesIO(customer_bytes)
)

print(f"Rows: {len(customer_df)}")

customer_df = standardize_date_column(
    customer_df,
    "registration_date"
)

customer_df = add_audit_columns(
    customer_df,
    "CRM"
)

upload_parquet(
    customer_df,
    f"crm/{year}/{month}/{day}/customers.parquet"
)

print("CRM Bronze completed.")


# ============================================================
# 4. PRODUCTS
# ============================================================

print("\n[4/9] Processing Products...")

product_key = get_latest_object(
    "products/",
    ".csv"
)

print(f"Raw source: {product_key}")

product_bytes = download_object(
    product_key
)

product_df = pd.read_csv(
    io.BytesIO(product_bytes)
)

print(f"Rows: {len(product_df)}")

product_df = standardize_numeric_column(
    product_df,
    "unit_cost"
)

product_df = standardize_numeric_column(
    product_df,
    "selling_price"
)

product_df = add_audit_columns(
    product_df,
    "PRODUCT_CATALOGUE"
)

upload_parquet(
    product_df,
    f"products/{year}/{month}/{day}/products.parquet"
)

print("Products Bronze completed.")


# ============================================================
# 5. STORES
# ============================================================

print("\n[5/9] Processing Stores...")

store_key = get_latest_object(
    "stores/",
    ".csv"
)

print(f"Raw source: {store_key}")

store_bytes = download_object(
    store_key
)

store_df = pd.read_csv(
    io.BytesIO(store_bytes)
)

print(f"Rows: {len(store_df)}")

store_df = add_audit_columns(
    store_df,
    "STORE_MASTER"
)

upload_parquet(
    store_df,
    f"stores/{year}/{month}/{day}/stores.parquet"
)

print("Stores Bronze completed.")


# ============================================================
# 6. INVENTORY
# ============================================================

print("\n[6/9] Processing Inventory...")

inventory_key = get_latest_object(
    "inventory/",
    ".csv"
)

print(f"Raw source: {inventory_key}")

inventory_bytes = download_object(
    inventory_key
)

inventory_df = pd.read_csv(
    io.BytesIO(inventory_bytes)
)

print(f"Rows: {len(inventory_df)}")

inventory_df = standardize_date_column(
    inventory_df,
    "snapshot_date"
)

inventory_df = standardize_numeric_column(
    inventory_df,
    "stock_quantity"
)

inventory_df = standardize_numeric_column(
    inventory_df,
    "reorder_level"
)

inventory_df = add_audit_columns(
    inventory_df,
    "INVENTORY"
)

upload_parquet(
    inventory_df,
    f"inventory/{year}/{month}/{day}/inventory.parquet"
)

print("Inventory Bronze completed.")


# ============================================================
# 7. SUPPLIER DELIVERIES
# ============================================================

print("\n[7/9] Processing Supplier Deliveries...")

supplier_key = get_latest_object(
    "suppliers/",
    ".json"
)

print(f"Raw source: {supplier_key}")

supplier_bytes = download_object(
    supplier_key
)

supplier_df = pd.read_json(
    io.BytesIO(supplier_bytes)
)

print(f"Rows: {len(supplier_df)}")

supplier_df = standardize_date_column(
    supplier_df,
    "delivery_date"
)

supplier_df = standardize_date_column(
    supplier_df,
    "expected_date"
)

supplier_df = standardize_numeric_column(
    supplier_df,
    "quantity"
)

supplier_df = add_audit_columns(
    supplier_df,
    "SUPPLIER_DELIVERIES"
)

upload_parquet(
    supplier_df,
    f"suppliers/{year}/{month}/{day}/supplier_deliveries.parquet"
)

print("Supplier Deliveries Bronze completed.")


# ============================================================
# 8. OPEN FOOD FACTS API
# ============================================================

print("\n[8/9] Processing Open Food Facts API data...")

try:
    open_food_facts_key = get_latest_object(
        "api/open_food_facts/",
        ".json"
    )

    print(
        f"Raw API source: {open_food_facts_key}"
    )

    open_food_facts_bytes = download_object(
        open_food_facts_key
    )

    open_food_facts_data = json.loads(
        open_food_facts_bytes.decode("utf-8")
    )

    products = open_food_facts_data.get(
        "products",
        []
    )

    if not products:
        raise ValueError(
            "No products found in the Open Food Facts API response."
        )

    open_food_facts_df = pd.DataFrame(
        products
    )

    print(
        f"Open Food Facts API records: "
        f"{len(open_food_facts_df)}"
    )

    open_food_facts_df = add_audit_columns(
        open_food_facts_df,
        "OPEN_FOOD_FACTS_API"
    )

    upload_parquet(
        open_food_facts_df,
        (
            f"api/open_food_facts/{year}/{month}/{day}/"
            f"open_food_facts.parquet"
        )
    )

    print(
        "Open Food Facts API Bronze completed."
    )

except Exception as error:
    print(
        f"Open Food Facts API Bronze processing failed: {error}"
    )


# ============================================================
# 9. DUMMYJSON API
# ============================================================

print("\n[9/9] Processing DummyJSON API data...")

try:
    dummyjson_key = get_latest_object(
        "api/dummyjson/",
        ".json"
    )

    print(f"Raw API source: {dummyjson_key}")

    dummyjson_bytes = download_object(dummyjson_key)

    dummyjson_data = json.loads(
        dummyjson_bytes.decode("utf-8")
    )

    dummyjson_products = dummyjson_data.get(
        "products",
        []
    )

    if not dummyjson_products:
        raise ValueError(
            "No products found in the DummyJSON API response."
        )

    dummyjson_df = pd.DataFrame(dummyjson_products)

    print(
        f"DummyJSON API records: {len(dummyjson_df)}"
    )

    dummyjson_df = add_audit_columns(
        dummyjson_df,
        "DUMMYJSON_API"
    )

    upload_parquet(
        dummyjson_df,
        (
            f"api/dummyjson/{year}/{month}/{day}/"
            "dummyjson_products.parquet"
        )
    )

    print("DummyJSON API Bronze completed.")

except Exception as error:
    print(
        f"DummyJSON API Bronze processing failed: {error}"
    )


# ============================================================
# FINISHED
# ============================================================

print("\n" + "=" * 60)
print("ALL BRONZE PROCESSING COMPLETED")
print("=" * 60)

print("\nBronze files created:")

print(
    f"  pos/{year}/{month}/{day}/pos_sales.parquet"
)

print(
    f"  ecommerce/{year}/{month}/{day}/ecommerce_orders.parquet"
)

print(
    f"  crm/{year}/{month}/{day}/customers.parquet"
)

print(
    f"  products/{year}/{month}/{day}/products.parquet"
)

print(
    f"  stores/{year}/{month}/{day}/stores.parquet"
)

print(
    f"  inventory/{year}/{month}/{day}/inventory.parquet"
)

print(
    f"  suppliers/{year}/{month}/{day}/supplier_deliveries.parquet"
)

print(
    f"  api/open_food_facts/{year}/{month}/{day}/"
    f"open_food_facts.parquet"
)

print(
    f"  api/dummyjson/{year}/{month}/{day}/"
    f"dummyjson_products.parquet"
)

print("\nBronze layer is ready.")