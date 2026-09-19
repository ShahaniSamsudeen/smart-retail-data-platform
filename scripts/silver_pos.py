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

RAW_BUCKET = "retaillake-raw"
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
# FIND LATEST BRONZE POS FILE
# ============================================================

def get_latest_pos_file():

    response = s3.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix="pos/"
    )

    objects = response.get("Contents", [])

    objects = [
        obj for obj in objects
        if obj["Key"].endswith(".parquet")
    ]

    if not objects:
        raise FileNotFoundError(
            "No Bronze POS Parquet file found."
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
print("RetailLake Silver Layer - POS Sales")
print("=" * 60)


# ------------------------------------------------------------
# Find Bronze file
# ------------------------------------------------------------

bronze_key = get_latest_pos_file()

print(f"Bronze source: {bronze_key}")


# ------------------------------------------------------------
# Load Bronze data
# ------------------------------------------------------------

df = download_parquet(bronze_key)

print(f"Bronze rows: {len(df)}")


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\nRunning Silver quality checks...")


# Create a list to store error reasons
df["_error_reason"] = ""


# ------------------------------------------------------------
# 1. Missing transaction ID
# ------------------------------------------------------------

mask = (
    df["transaction_id"].isna()
    |
    (df["transaction_id"].astype(str).str.strip() == "")
)

df.loc[
    mask,
    "_error_reason"
] = "Missing transaction_id"


# ------------------------------------------------------------
# 2. Duplicate transaction ID
# ------------------------------------------------------------

duplicate_mask = df.duplicated(
    subset=["transaction_id"],
    keep=False
)

df.loc[
    duplicate_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Duplicate transaction_id"


# ------------------------------------------------------------
# 3. Missing store ID
# ------------------------------------------------------------

mask = (
    df["store_id"].isna()
    |
    (df["store_id"].astype(str).str.strip() == "")
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing store_id"


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


# ------------------------------------------------------------
# 5. Unknown product IDs
# ------------------------------------------------------------

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

unknown_product_mask = (
    ~df["product_id"].astype(str).isin(valid_products)
)

df.loc[
    unknown_product_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Unknown product_id"


# ------------------------------------------------------------
# 6. Negative or zero quantity
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
# 7. Negative or zero price
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
# 8. Invalid/future transaction date
# ------------------------------------------------------------

today = pd.Timestamp.now().normalize()

mask = (
    df["transaction_date"].isna()
    |
    (df["transaction_date"] > today)
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Invalid or future transaction_date"


# ------------------------------------------------------------
# 9. Missing customer ID
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
# UPLOAD RESULTS
# ============================================================

processing_date = datetime.now()

year = processing_date.strftime("%Y")
month = processing_date.strftime("%m")
day = processing_date.strftime("%d")


clean_key = (
    f"pos/{year}/{month}/{day}/"
    f"pos_sales_clean.parquet"
)

quarantine_key = (
    f"quarantine/pos/{year}/{month}/{day}/"
    f"pos_sales_quarantine.parquet"
)

quality_key = (
    f"quality/pos/{year}/{month}/{day}/"
    f"quality_summary.json"
)


upload_parquet(
    clean_df,
    clean_key
)

upload_parquet(
    quarantine_df,
    quarantine_key
)


# ============================================================
# QUALITY SUMMARY
# ============================================================

quality_summary = {
    "source": "POS",
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
print("POS SILVER PROCESSING RESULTS")
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

print("\nPOS Silver processing completed successfully.")


