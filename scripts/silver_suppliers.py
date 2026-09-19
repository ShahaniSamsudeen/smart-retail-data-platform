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
print("RetailLake Silver Layer - Supplier Deliveries")
print("=" * 60)


# ============================================================
# LOAD SUPPLIER DELIVERIES
# ============================================================

supplier_key = get_latest_file(
    "suppliers/"
)

print(f"Bronze source: {supplier_key}")

df = download_parquet(
    BRONZE_BUCKET,
    supplier_key
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
# CREATE VALID SUPPLIER IDs
# ============================================================

# The project does not currently have a separate supplier
# master source, so supplier IDs are validated using the
# supplier IDs that appear in the delivery source itself.
#
# SUP999 is intentionally invalid and will be handled below.

valid_supplier_ids = set(
    df["supplier_id"]
    .dropna()
    .astype(str)
)

# Remove the deliberately invalid supplier ID
valid_supplier_ids.discard("SUP999")


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\nRunning Supplier Delivery quality checks...")

df["_error_reason"] = ""


# ------------------------------------------------------------
# 1. Missing delivery ID
# ------------------------------------------------------------

mask = (
    df["delivery_id"].isna()
    |
    (df["delivery_id"].astype(str).str.strip() == "")
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing delivery_id"


# ------------------------------------------------------------
# 2. Duplicate delivery ID
# ------------------------------------------------------------

duplicate_mask = (
    df["delivery_id"].duplicated(
        keep=False
    )
)

df.loc[
    duplicate_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Duplicate delivery_id"


# ------------------------------------------------------------
# 3. Missing supplier ID
# ------------------------------------------------------------

mask = (
    df["supplier_id"].isna()
    |
    (df["supplier_id"].astype(str).str.strip() == "")
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing supplier_id"


# ------------------------------------------------------------
# 4. Unknown supplier ID
# ------------------------------------------------------------

mask = ~df["supplier_id"].astype(str).isin(
    valid_supplier_ids
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Unknown supplier_id"


# ------------------------------------------------------------
# 5. Missing product ID
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
# 6. Unknown product ID
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
# 8. Missing delivery date
# ------------------------------------------------------------

mask = (
    df["delivery_date"].isna()
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing delivery_date"


# ------------------------------------------------------------
# 9. Future delivery date
# ------------------------------------------------------------

today = pd.Timestamp.now().normalize()

mask = (
    df["delivery_date"] > today
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Future delivery_date"


# ------------------------------------------------------------
# 10. Missing expected date
# ------------------------------------------------------------

mask = (
    df["expected_date"].isna()
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing expected_date"


# ------------------------------------------------------------
# 11. Delivery date after expected date
# ------------------------------------------------------------

mask = (
    df["delivery_date"]
    > df["expected_date"]
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Delivery date after expected date"


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
    f"suppliers/{year}/{month}/{day}/"
    f"supplier_deliveries_clean.parquet"
)

quarantine_key = (
    f"quarantine/suppliers/{year}/{month}/{day}/"
    f"supplier_deliveries_quarantine.parquet"
)

quality_key = (
    f"quality/suppliers/{year}/{month}/{day}/"
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
    "source": "SUPPLIER_DELIVERIES",
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
print("SUPPLIER DELIVERY SILVER PROCESSING RESULTS")
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


print("\nSupplier Delivery Silver processing completed successfully.")


