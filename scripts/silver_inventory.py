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
# FIND LATEST BRONZE FILE
# ============================================================

def get_latest_file(prefix):

    response = s3.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix=prefix
    )

    objects = response.get("Contents", [])

    objects = [
        obj for obj in objects
        if obj["Key"].endswith(".parquet")
    ]

    if not objects:
        raise FileNotFoundError(
            f"No Bronze Parquet file found under {prefix}"
        )

    return max(
        objects,
        key=lambda obj: obj["LastModified"]
    )["Key"]


# ============================================================
# DOWNLOAD PARQUET
# ============================================================

def download_parquet(bucket, object_key):

    response = s3.get_object(
        Bucket=bucket,
        Key=object_key
    )

    return pd.read_parquet(
        io.BytesIO(response["Body"].read())
    )


# ============================================================
# UPLOAD PARQUET
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
# MAIN
# ============================================================

print("\n" + "=" * 60)
print("RetailLake Silver Layer - Inventory")
print("=" * 60)


# ============================================================
# LOAD INVENTORY
# ============================================================

inventory_key = get_latest_file(
    "inventory/"
)

print(f"Bronze source: {inventory_key}")

df = download_parquet(
    BRONZE_BUCKET,
    inventory_key
)

print(f"Bronze rows: {len(df)}")


# ============================================================
# LOAD VALID PRODUCT IDs
# ============================================================

product_key = get_latest_file(
    "products/"
)

products_df = download_parquet(
    BRONZE_BUCKET,
    product_key
)

valid_product_ids = set(
    products_df["product_id"]
    .dropna()
    .astype(str)
)


# ============================================================
# LOAD VALID STORE IDs
# ============================================================

store_key = get_latest_file(
    "stores/"
)

stores_df = download_parquet(
    BRONZE_BUCKET,
    store_key
)

valid_store_ids = set(
    stores_df["store_id"]
    .dropna()
    .astype(str)
)


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\nRunning Inventory quality checks...")

df["_error_reason"] = ""


# ------------------------------------------------------------
# 1. Missing snapshot date
# ------------------------------------------------------------

mask = (
    df["snapshot_date"].isna()
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing snapshot_date"


# ------------------------------------------------------------
# 2. Future snapshot date
# ------------------------------------------------------------

today = pd.Timestamp.now().normalize()

mask = (
    df["snapshot_date"] > today
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Future snapshot_date"


# ------------------------------------------------------------
# 3. Missing product ID
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
# 4. Unknown product ID
# ------------------------------------------------------------

mask = ~df["product_id"].astype(str).isin(
    valid_product_ids
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Unknown product_id"


# ------------------------------------------------------------
# 5. Missing store ID
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
# 6. Unknown store ID
# ------------------------------------------------------------

mask = ~df["store_id"].astype(str).isin(
    valid_store_ids
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Unknown store_id"


# ------------------------------------------------------------
# 7. Negative stock quantity
# ------------------------------------------------------------

mask = (
    df["stock_quantity"].isna()
    |
    (df["stock_quantity"] < 0)
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Invalid stock_quantity"


# ------------------------------------------------------------
# 8. Invalid reorder level
# ------------------------------------------------------------

mask = (
    df["reorder_level"].isna()
    |
    (df["reorder_level"] < 0)
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Invalid reorder_level"


# ------------------------------------------------------------
# 9. Check stock status
# ------------------------------------------------------------

expected_status = []

for _, row in df.iterrows():

    stock = row["stock_quantity"]
    reorder = row["reorder_level"]

    if pd.isna(stock) or pd.isna(reorder):

        expected_status.append(None)

    elif stock == 0:

        expected_status.append("Out of Stock")

    elif stock <= reorder:

        expected_status.append("Low Stock")

    else:

        expected_status.append("In Stock")


status_mismatch = (
    df["stock_status"].astype(str)
    != pd.Series(
        expected_status,
        index=df.index
    ).astype(str)
)

df.loc[
    status_mismatch &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Incorrect stock_status"


# ============================================================
# SEPARATE CLEAN AND QUARANTINED DATA
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
    f"inventory/{year}/{month}/{day}/"
    f"inventory_clean.parquet"
)

quarantine_key = (
    f"quarantine/inventory/{year}/{month}/{day}/"
    f"inventory_quarantine.parquet"
)

quality_key = (
    f"quality/inventory/{year}/{month}/{day}/"
    f"quality_summary.json"
)


# ============================================================
# UPLOAD CLEAN DATA
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
    "source": "INVENTORY",
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
print("INVENTORY SILVER PROCESSING RESULTS")
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


print("\nInventory Silver processing completed successfully.")


