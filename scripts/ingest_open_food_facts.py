import json
import os
import time
from datetime import datetime, timezone

import boto3
import requests
from dotenv import load_dotenv
from botocore.client import Config

load_dotenv()

# API configuration
API_URL = "https://world.openfoodfacts.org/api/v2/search"

# MinIO configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "retaillake_admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "retaillake_password")
RAW_BUCKET = "retaillake-raw"

# Open Food Facts requires a descriptive User-Agent
HEADERS = {
    "User-Agent": "RetailLakeDataPlatform/1.0 (retail-data-project)"
}


def fetch_open_food_facts():
    """Fetch product data from the Open Food Facts API with retries."""

    params = {
        "categories_tags_en": "beverages",
        "page_size": 100,
        "page": 1,
        "fields": (
            "code,product_name,brands,categories,"
            "quantity,nutriscore_grade,countries"
        ),
    }

    print("Fetching data from Open Food Facts API...")

    response = None

    for attempt in range(1, 4):
        try:
            response = requests.get(
                API_URL,
                params=params,
                headers=HEADERS,
                timeout=60,
            )

            if response.status_code == 503:
                print(
                    f"API temporarily unavailable. "
                    f"Retrying in 10 seconds... ({attempt}/3)"
                )

                if attempt < 3:
                    time.sleep(10)
                    continue

            response.raise_for_status()
            break

        except requests.RequestException as error:
            if attempt == 3:
                raise error

            print(
                f"API request failed: {error}. "
                f"Retrying in 10 seconds... ({attempt}/3)"
            )
            time.sleep(10)

    if response is None:
        raise RuntimeError("No response received from the API.")

    data = response.json()

    products = data.get("products", [])

    print(
        f"API request successful. "
        f"Products received: {len(products)}"
    )

    return data


def upload_to_minio(data):
    """Upload the API response to the RetailLake Raw layer."""

    timestamp = datetime.now(timezone.utc)
    date_path = timestamp.strftime("%Y/%m/%d")

    object_key = (
        f"api/open_food_facts/{date_path}/"
        f"open_food_facts.json"
    )

    s3_client = boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )

    json_data = json.dumps(
        data,
        ensure_ascii=False,
        indent=2,
    )

    s3_client.put_object(
        Bucket=RAW_BUCKET,
        Key=object_key,
        Body=json_data.encode("utf-8"),
        ContentType="application/json",
    )

    print("API data uploaded successfully to MinIO.")
    print(f"Bucket: {RAW_BUCKET}")
    print(f"Object key: {object_key}")


def main():
    try:
        data = fetch_open_food_facts()
        upload_to_minio(data)

        print(
            "Open Food Facts API ingestion "
            "completed successfully."
        )

    except requests.RequestException as error:
        print(f"API request failed after retries: {error}")

    except Exception as error:
        print(f"Ingestion failed: {error}")


if __name__ == "__main__":
    main()