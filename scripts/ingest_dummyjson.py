
import io
import json
import os
from datetime import datetime

import boto3
import requests
from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000"
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY",
    "retaillake_admin"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY",
    "retaillake_password"
)

RAW_BUCKET = "retaillake-raw"

API_URL = "https://dummyjson.com/products"


# ============================================================
# START
# ============================================================

print()
print("=" * 60)
print("RetailLake - DummyJSON API Ingestion")
print("=" * 60)


# ============================================================
# REQUEST DATA FROM DUMMYJSON API
# ============================================================

print()
print("Fetching product data from DummyJSON API...")

try:
    response = requests.get(
        API_URL,
        params={
            "limit": 100
        },
        timeout=30
    )

    response.raise_for_status()

    api_data = response.json()

    products = api_data.get("products", [])

    print(
        f"API request successful. Products received: {len(products)}"
    )

except requests.RequestException as error:
    raise RuntimeError(
        f"DummyJSON API request failed: {error}"
    ) from error


if not products:
    raise ValueError(
        "No products were returned by the DummyJSON API."
    )


# ============================================================
# CONNECT TO MINIO
# ============================================================

print()
print("Connecting to MinIO...")

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)

print("MinIO connection successful!")


# ============================================================
# CREATE RAW BUCKET IF REQUIRED
# ============================================================

try:
    s3.head_bucket(
        Bucket=RAW_BUCKET
    )

except Exception:
    s3.create_bucket(
        Bucket=RAW_BUCKET
    )

    print(
        f"Created bucket: {RAW_BUCKET}"
    )


# ============================================================
# PREPARE RAW API DATA
# ============================================================

today = datetime.now()

date_path = today.strftime("%Y/%m/%d")

object_key = (
    f"api/dummyjson/{date_path}/dummyjson_products.json"
)

raw_payload = {
    "source": "DummyJSON API",
    "api_url": API_URL,
    "retrieved_at": today.isoformat(),
    "product_count": len(products),
    "products": products
}

json_bytes = json.dumps(
    raw_payload,
    indent=2,
    ensure_ascii=False
).encode("utf-8")


# ============================================================
# UPLOAD TO RAW LAYER
# ============================================================

s3.put_object(
    Bucket=RAW_BUCKET,
    Key=object_key,
    Body=io.BytesIO(json_bytes),
    ContentType="application/json"
)

print()
print("DummyJSON API data uploaded successfully.")
print(f"Bucket: {RAW_BUCKET}")
print(f"Object key: {object_key}")


# ============================================================
# COMPLETION
# ============================================================

print()
print("DummyJSON API ingestion completed successfully.")