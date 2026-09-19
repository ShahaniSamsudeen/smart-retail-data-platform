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

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = "retaillake_admin"
MINIO_SECRET_KEY = "retaillake_password"

BRONZE_BUCKET = "retaillake-bronze"
SILVER_BUCKET = "retaillake-silver"


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
# FIND LATEST BRONZE E-COMMERCE FILE
# ============================================================

def get_latest_ecommerce_file():

    response = s3.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix="ecommerce/"
    )

    objects = response.get("Contents", [])

    objects = [
        obj for obj in objects
        if obj["Key"].endswith(".parquet")
    ]

    if not objects:
        raise FileNotFoundError(
            "No Bronze E-commerce Parquet file found."
        )

    latest = max(
        objects,
        key=lambda obj: obj["LastModified"]
    )

    return latest["Key"]


# ============================================================
# DOWNLOAD PARQUET
# ============================================================

def download_parquet(object_key):

    response = s3.get_object(
        Bucket=BRONZE_BUCKET,
        Key=object_key
    )

    return pd.read_parquet(
        io.BytesIO(response["Body"].read())
    )


# ============================================================
# UPLOAD DATAFRAME AS PARQUET
# ============================================================

def upload_parquet(df, object_key):

    buffer = io.BytesIO()

    df.to_parquet(
        buffer,
        index=False,
        engine="pyarrow"
    )

    buffer.seek(0)

    s3.put_object(
        Bucket=SILVER_BUCKET,
        Key=object_key,
        Body=buffer.getvalue(),
        ContentType="application/octet-stream"
    )


# ============================================================
# UPLOAD JSON
# ============================================================

def upload_json(data, object_key):

    body = json.dumps(
        data,
        indent=4
    )

    s3.put_object(
        Bucket=SILVER_BUCKET,
        Key=object_key,
        Body=body.encode("utf-8"),
        ContentType="application/json"
    )


# ============================================================
# MAIN SILVER PROCESS
# ============================================================

print("\n" + "=" * 60)
print("RetailLake Silver Layer - E-commerce Orders")
print("=" * 60)


# ------------------------------------------------------------
# Find Bronze file
# ------------------------------------------------------------

bronze_key = get_latest_ecommerce_file()

print(f"Bronze source: {bronze_key}")


# ------------------------------------------------------------
# Load Bronze data
# ------------------------------------------------------------

df = download_parquet(bronze_key)

print(f"Bronze rows: {len(df)}")


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\nRunning E-commerce quality checks...")

df["_error_reason"] = ""


# ------------------------------------------------------------
# 1. Missing order ID
# ------------------------------------------------------------

mask = (
    df["order_id"].isna()
    |
    (df["order_id"].astype(str).str.strip() == "")
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing order_id"


# ------------------------------------------------------------
# 2. Duplicate order ID
# ------------------------------------------------------------

duplicate_mask = df.duplicated(
    subset=["order_id"],
    keep=False
)

df.loc[
    duplicate_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Duplicate order_id"


# ------------------------------------------------------------
# 3. Missing customer ID
# ------------------------------------------------------------

mask = (
    df["customer_id"].isna()
    |
    (df["customer_id"].astype(str).str.strip() == "")
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing customer_id"


# ------------------------------------------------------------
# 4. Missing product ID
# ------------------------------------------------------------

mask = (
    df["product_id"].isna()
    |
    (df["product_id"].astype(str).str.strip() == "")
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing product_id"


# ============================================================
# LOAD VALID PRODUCT IDs
# ============================================================

product_response = s3.list_objects_v2(
    Bucket=BRONZE_BUCKET,
    Prefix="products/"
)

product_objects = [
    obj for obj in product_response.get("Contents", [])
    if obj["Key"].endswith(".parquet")
]

latest_product = max(
    product_objects,
    key=lambda obj: obj["LastModified"]
)

product_response = s3.get_object(
    Bucket=BRONZE_BUCKET,
    Key=latest_product["Key"]
)

products_df = pd.read_parquet(
    io.BytesIO(product_response["Body"].read())
)

valid_products = set(
    products_df["product_id"]
    .dropna()
    .astype(str)
)


# ------------------------------------------------------------
# 5. Unknown product ID
# ------------------------------------------------------------

unknown_product_mask = (
    ~df["product_id"].astype(str).isin(valid_products)
)

df.loc[
    unknown_product_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Unknown product_id"


# ============================================================
# LOAD VALID CUSTOMER IDs
# ============================================================

customer_response = s3.list_objects_v2(
    Bucket=BRONZE_BUCKET,
    Prefix="crm/"
)

customer_objects = [
    obj for obj in customer_response.get("Contents", [])
    if obj["Key"].endswith(".parquet")
]

latest_customer = max(
    customer_objects,
    key=lambda obj: obj["LastModified"]
)

customer_response = s3.get_object(
    Bucket=BRONZE_BUCKET,
    Key=latest_customer["Key"]
)

customers_df = pd.read_parquet(
    io.BytesIO(customer_response["Body"].read())
)

valid_customers = set(
    customers_df["customer_id"]
    .dropna()
    .astype(str)
)


# ------------------------------------------------------------
# 6. Unknown customer ID
# ------------------------------------------------------------

unknown_customer_mask = (
    ~df["customer_id"].astype(str).isin(valid_customers)
)

df.loc[
    unknown_customer_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Unknown customer_id"


# ------------------------------------------------------------
# 7. Invalid quantity
# ------------------------------------------------------------

mask = (
    df["quantity"].isna()
    |
    (df["quantity"] <= 0)
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Invalid quantity"


# ------------------------------------------------------------
# 8. Invalid unit price
# ------------------------------------------------------------

mask = (
    df["unit_price"].isna()
    |
    (df["unit_price"] <= 0)
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Invalid unit_price"


# ------------------------------------------------------------
# 9. Invalid or future order date
# ------------------------------------------------------------

today = pd.Timestamp.now().normalize()

mask = (
    df["order_date"].isna()
    |
    (df["order_date"] > today)
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Invalid or future order_date"


# ============================================================
# SEPARATE CLEAN AND QUARANTINED RECORDS
# ============================================================

quarantine_df = df[
    df["_error_reason"] != ""
].copy()

clean_df = df[
    df["_error_reason"] == ""
].copy()


# ============================================================
# REMOVE ERROR COLUMN FROM CLEAN DATA
# ============================================================

clean_df = clean_df.drop(
    columns=["_error_reason"]
)


# ============================================================
# ADD SILVER METADATA
# ============================================================

clean_df["_silver_processed_at"] = (
    datetime.now().isoformat()
)

quarantine_df["_quarantined_at"] = (
    datetime.now().isoformat()
)


# ============================================================
# PROCESSING DATE
# ============================================================

processing_date = datetime.now()

year = processing_date.strftime("%Y")
month = processing_date.strftime("%m")
day = processing_date.strftime("%d")


# ============================================================
# OUTPUT PATHS
# ============================================================

clean_key = (
    f"ecommerce/{year}/{month}/{day}/"
    f"ecommerce_orders_clean.parquet"
)

quarantine_key = (
    f"quarantine/ecommerce/{year}/{month}/{day}/"
    f"ecommerce_orders_quarantine.parquet"
)

quality_key = (
    f"quality/ecommerce/{year}/{month}/{day}/"
    f"quality_summary.json"
)


# ============================================================
# UPLOAD CLEAN SILVER DATA
# ============================================================

upload_parquet(
    clean_df,
    clean_key
)


# ============================================================
# UPLOAD QUARANTINED DATA
# ============================================================

upload_parquet(
    quarantine_df,
    quarantine_key
)


# ============================================================
# QUALITY SUMMARY
# ============================================================

quality_summary = {
    "source": "E_COMMERCE",
    "processing_time": datetime.now().isoformat(),
    "bronze_rows": int(len(df)),
    "clean_rows": int(len(clean_df)),
    "quarantined_rows": int(len(quarantine_df)),
    "quarantine_percentage": round(
        (len(quarantine_df) / len(df)) * 100,
        2
    ),
    "quality_status": "PASS",
    "error_counts": {
        str(reason): int(count)
        for reason, count in
        quarantine_df["_error_reason"]
        .value_counts()
        .items()
    }
}


upload_json(
    quality_summary,
    quality_key
)


# ============================================================
# DISPLAY RESULTS
# ============================================================

print("\n" + "=" * 60)
print("E-COMMERCE SILVER PROCESSING RESULTS")
print("=" * 60)

print(f"Bronze rows:       {len(df)}")
print(f"Clean rows:        {len(clean_df)}")
print(f"Quarantined rows:  {len(quarantine_df)}")

print(
    f"Quarantine rate:   "
    f"{quality_summary['quarantine_percentage']}%"
)


print("\nQuality issues found:")

if len(quarantine_df) > 0:

    print(
        quarantine_df["_error_reason"]
        .value_counts()
        .to_string()
    )

else:

    print("No quality issues found.")


print("\nSilver files created:")

print(f"  {clean_key}")
print(f"  {quarantine_key}")
print(f"  {quality_key}")


print("\nE-commerce Silver processing completed successfully.")


