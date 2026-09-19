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
# FIND LATEST BRONZE STORES FILE
# ============================================================

def get_latest_store_file():

    response = s3.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix="stores/"
    )

    objects = response.get("Contents", [])

    objects = [
        obj for obj in objects
        if obj["Key"].endswith(".parquet")
    ]

    if not objects:
        raise FileNotFoundError(
            "No Bronze Stores Parquet file found."
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
print("RetailLake Silver Layer - Stores")
print("=" * 60)


# ------------------------------------------------------------
# Find Bronze file
# ------------------------------------------------------------

bronze_key = get_latest_store_file()

print(f"Bronze source: {bronze_key}")


# ------------------------------------------------------------
# Load Bronze data
# ------------------------------------------------------------

df = download_parquet(bronze_key)

print(f"Bronze rows: {len(df)}")


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\nRunning Store quality checks...")

df["_error_reason"] = ""


# ------------------------------------------------------------
# 1. Missing store ID
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
# 2. Duplicate store ID
# ------------------------------------------------------------

duplicate_mask = df.duplicated(
    subset=["store_id"],
    keep=False
)

df.loc[
    duplicate_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Duplicate store_id"


# ------------------------------------------------------------
# 3. Missing store name
# ------------------------------------------------------------

mask = (
    df["store_name"].isna()
    |
    (df["store_name"].astype(str).str.strip() == "")
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing store_name"


# ------------------------------------------------------------
# 4. Missing city
# ------------------------------------------------------------

mask = (
    df["city"].isna()
    |
    (df["city"].astype(str).str.strip() == "")
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing city"


# ------------------------------------------------------------
# 5. Missing region
# ------------------------------------------------------------

mask = (
    df["region"].isna()
    |
    (df["region"].astype(str).str.strip() == "")
)

df.loc[
    mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Missing region"


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
    f"stores/{year}/{month}/{day}/"
    f"stores_clean.parquet"
)

quarantine_key = (
    f"quarantine/stores/{year}/{month}/{day}/"
    f"stores_quarantine.parquet"
)

quality_key = (
    f"quality/stores/{year}/{month}/{day}/"
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
    "source": "STORES",
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
print("STORE SILVER PROCESSING RESULTS")
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


print("\nStore Silver processing completed successfully.")


