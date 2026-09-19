
import io
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
from dotenv import load_dotenv


# ============================================================
# CONFIGURATION
# ============================================================

load_dotenv()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "retaillake_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "retaillake_password")

BRONZE_BUCKET = "retaillake-bronze"
SILVER_BUCKET = "retaillake-silver"

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)


# ============================================================
# HELPER FUNCTIONS
# ============================================================

def get_latest_api_file():
    """Find the latest DummyJSON Bronze Parquet file."""

    response = s3.list_objects_v2(
        Bucket=BRONZE_BUCKET,
        Prefix="api/dummyjson/"
    )

    files = [
        obj["Key"]
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    if not files:
        raise FileNotFoundError(
            "No DummyJSON Bronze Parquet file was found."
        )

    return sorted(files)[-1]


def upload_parquet(df, bucket, key):
    """Upload a DataFrame as a Parquet file to MinIO."""

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    s3.put_object(
        Bucket=bucket,
        Key=key,
        Body=buffer.getvalue()
    )


# ============================================================
# MAIN PROCESSING
# ============================================================

print("=" * 60)
print("RetailLake Silver Layer - DummyJSON API")
print("=" * 60)

bronze_key = get_latest_api_file()

print(f"\nBronze API source: {bronze_key}")

response = s3.get_object(
    Bucket=BRONZE_BUCKET,
    Key=bronze_key
)

df = pd.read_parquet(
    io.BytesIO(response["Body"].read())
)

print(f"Bronze API rows: {len(df)}")

# Preserve the original data for quality reporting
original_count = len(df)

# Standardize column names
df.columns = [
    column.strip().lower().replace(" ", "_")
    for column in df.columns
]

# Ensure expected columns exist
expected_columns = [
    "id",
    "title",
    "description",
    "category",
    "price",
    "discount_percentage",
    "rating",
    "stock",
    "brand",
    "sku",
]

for column in expected_columns:
    if column not in df.columns:
        df[column] = None

# Keep only relevant columns
df = df[expected_columns].copy()

# Clean text columns
text_columns = [
    "title",
    "description",
    "category",
    "brand",
    "sku",
]

for column in text_columns:
    df[column] = (
        df[column]
        .fillna("")
        .astype(str)
        .str.strip()
    )

# Convert empty strings to missing values
df = df.replace("", pd.NA)

# Convert numeric columns
numeric_columns = [
    "id",
    "price",
    "discount_percentage",
    "rating",
    "stock",
]

for column in numeric_columns:
    df[column] = pd.to_numeric(
        df[column],
        errors="coerce"
    )

# Create quality flags
df["quality_issue"] = ""

# Missing product ID
df.loc[
    df["id"].isna(),
    "quality_issue"
] = "Missing product ID"

# Missing product title
df.loc[
    df["title"].isna()
    & (df["quality_issue"] == ""),
    "quality_issue"
] = "Missing product title"

# Invalid price
df.loc[
    (df["price"].notna())
    & (df["price"] < 0)
    & (df["quality_issue"] == ""),
    "quality_issue"
] = "Negative product price"

# Invalid stock
df.loc[
    (df["stock"].notna())
    & (df["stock"] < 0)
    & (df["quality_issue"] == ""),
    "quality_issue"
] = "Negative stock quantity"

# Invalid rating
df.loc[
    (df["rating"].notna())
    & (
        (df["rating"] < 0)
        | (df["rating"] > 5)
    )
    & (df["quality_issue"] == ""),
    "quality_issue"
] = "Invalid product rating"

# Detect duplicate product IDs
duplicate_mask = (
    df["id"].notna()
    & df["id"].duplicated(keep="first")
)

df.loc[
    duplicate_mask
    & (df["quality_issue"] == ""),
    "quality_issue"
] = "Duplicate product ID"

# Separate clean and quarantined records
quarantine_df = df[
    df["quality_issue"] != ""
].copy()

clean_df = df[
    df["quality_issue"] == ""
].copy()

# Remove quality column from clean dataset
clean_df = clean_df.drop(
    columns=["quality_issue"]
)

# Add audit columns
processed_at = datetime.now(
    timezone.utc
).isoformat()

clean_df["_source"] = "DUMMYJSON_API"
clean_df["_processed_at"] = processed_at

quarantine_df["_source"] = "DUMMYJSON_API"
quarantine_df["_processed_at"] = processed_at

# Prepare output paths
today = datetime.now().strftime("%Y/%m/%d")

clean_key = (
    f"api/dummyjson/{today}/"
    "dummyjson_products_clean.parquet"
)

quarantine_key = (
    f"api/dummyjson/{today}/"
    "dummyjson_products_quarantine.parquet"
)

quality_key = (
    f"api/dummyjson/{today}/"
    "dummyjson_products_quality_summary.csv"
)

# Upload clean data
upload_parquet(
    clean_df,
    SILVER_BUCKET,
    clean_key
)

# Upload quarantine data if necessary
if len(quarantine_df) > 0:
    upload_parquet(
        quarantine_df,
        SILVER_BUCKET,
        quarantine_key
    )

# Create quality summary
quality_summary = pd.DataFrame([
    {
        "source": "DUMMYJSON_API",
        "bronze_rows": original_count,
        "clean_rows": len(clean_df),
        "quarantine_rows": len(quarantine_df),
        "status": "SUCCESS",
        "processed_at": processed_at,
    }
])

summary_buffer = io.BytesIO()
quality_summary.to_csv(
    summary_buffer,
    index=False
)
summary_buffer.seek(0)

s3.put_object(
    Bucket=SILVER_BUCKET,
    Key=quality_key,
    Body=summary_buffer.getvalue()
)

# Display results
print("\n" + "=" * 60)
print("DUMMYJSON SILVER RESULTS")
print("=" * 60)
print(f"Bronze rows:       {original_count}")
print(f"Clean rows:        {len(clean_df)}")
print(f"Quarantine rows:   {len(quarantine_df)}")
print(f"\nClean output:      {clean_key}")

if len(quarantine_df) > 0:
    print(f"Quarantine output: {quarantine_key}")

print(f"Quality summary:   {quality_key}")
print("\nDummyJSON Silver processing completed successfully.")