import io
import json
import hashlib
import re
import os
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
# FIND LATEST BRONZE CUSTOMER FILE
# ============================================================

def get_latest_customer_file():

    response = s3.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix="crm/"
    )

    objects = response.get("Contents", [])

    objects = [
        obj for obj in objects
        if obj["Key"].endswith(".parquet")
    ]

    if not objects:
        raise FileNotFoundError(
            "No Bronze customer Parquet file found."
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
# EMAIL VALIDATION
# ============================================================

def is_valid_email(email):

    if pd.isna(email):
        return False

    email = str(email).strip()

    pattern = r"^[^@\s]+@[^@\s]+\.[^@\s]+$"

    return bool(
        re.match(pattern, email)
    )


# ============================================================
# HASH EMAIL
# ============================================================

def hash_email(email):

    email = str(email).strip().lower()

    return hashlib.sha256(
        email.encode("utf-8")
    ).hexdigest()


# ============================================================
# MAIN SILVER PROCESS
# ============================================================

print("\n" + "=" * 60)
print("RetailLake Silver Layer - CRM Customers")
print("=" * 60)


# ------------------------------------------------------------
# Find Bronze file
# ------------------------------------------------------------

bronze_key = get_latest_customer_file()

print(f"Bronze source: {bronze_key}")


# ------------------------------------------------------------
# Load Bronze data
# ------------------------------------------------------------

df = download_parquet(bronze_key)

print(f"Bronze rows: {len(df)}")


# ============================================================
# DATA QUALITY CHECKS
# ============================================================

print("\nRunning CRM quality checks...")


# Create error column

df["_error_reason"] = ""


# ------------------------------------------------------------
# 1. Missing customer ID
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
# 2. Duplicate customer ID
# ------------------------------------------------------------

duplicate_mask = df.duplicated(
    subset=["customer_id"],
    keep=False
)

df.loc[
    duplicate_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Duplicate customer_id"


# ------------------------------------------------------------
# 3. Invalid email
# ------------------------------------------------------------

invalid_email_mask = ~df["email"].apply(
    is_valid_email
)

df.loc[
    invalid_email_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Invalid email"


# ------------------------------------------------------------
# 4. Future registration date
# ------------------------------------------------------------

today = pd.Timestamp.now().normalize()

future_date_mask = (
    df["registration_date"].isna()
    |
    (df["registration_date"] > today)
)

df.loc[
    future_date_mask &
    (df["_error_reason"] == ""),
    "_error_reason"
] = "Invalid or future registration_date"


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
# HASH CUSTOMER EMAIL
# ============================================================

print("\nHashing customer email addresses...")


clean_df["email_hash"] = clean_df["email"].apply(
    hash_email
)


# Remove original email from Silver

clean_df = clean_df.drop(
    columns=["email", "_error_reason"]
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
    f"crm/{year}/{month}/{day}/"
    f"customers_clean.parquet"
)

quarantine_key = (
    f"quarantine/crm/{year}/{month}/{day}/"
    f"customers_quarantine.parquet"
)

quality_key = (
    f"quality/crm/{year}/{month}/{day}/"
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
    "source": "CRM",
    "processing_time": datetime.now().isoformat(),
    "bronze_rows": int(len(df)),
    "clean_rows": int(len(clean_df)),
    "quarantined_rows": int(len(quarantine_df)),
    "quarantine_percentage": round(
        (len(quarantine_df) / len(df)) * 100,
        2
    ),
    "quality_status": "PASS",
    "pii_protection": {
        "email_hashed": True,
        "hash_algorithm": "SHA-256"
    },
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
print("CRM SILVER PROCESSING RESULTS")
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


print("\nPII protection:")
print("  Original email: REMOVED from clean Silver")
print("  Email hash:     SHA-256")


print("\nSilver files created:")

print(f"  {clean_key}")
print(f"  {quarantine_key}")
print(f"  {quality_key}")


print("\nCRM Silver processing completed successfully.")