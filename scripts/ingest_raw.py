from pathlib import Path
from datetime import datetime
import csv
import json
import os

import boto3
from botocore.exceptions import ClientError
from dotenv import load_dotenv


# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------

load_dotenv()


# ---------------------------------------------------------
# Project paths
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


# ---------------------------------------------------------
# MinIO configuration
# ---------------------------------------------------------

MINIO_ENDPOINT = os.getenv(
    "MINIO_ENDPOINT",
    "http://localhost:9000"
)

MINIO_ACCESS_KEY = os.getenv(
    "MINIO_ACCESS_KEY"
)

MINIO_SECRET_KEY = os.getenv(
    "MINIO_SECRET_KEY"
)

MINIO_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "retaillake-raw"
)


# ---------------------------------------------------------
# Source files
# ---------------------------------------------------------

SOURCE_FILES = {
    "stores": RAW_DATA_DIR / "stores" / "stores.csv",
    "products": RAW_DATA_DIR / "products" / "products.csv",
    "crm": RAW_DATA_DIR / "crm" / "customers.csv",
    "pos": RAW_DATA_DIR / "pos" / "pos_sales.csv",
    "ecommerce": RAW_DATA_DIR / "ecommerce" / "ecommerce_orders.json",
    "inventory": RAW_DATA_DIR / "inventory" / "inventory.csv",
    "suppliers": RAW_DATA_DIR / "suppliers" / "supplier_deliveries.json",
}


# ---------------------------------------------------------
# Create MinIO/S3 client
# ---------------------------------------------------------

def create_minio_client():
    if not MINIO_ACCESS_KEY or not MINIO_SECRET_KEY:
        raise ValueError(
            "MINIO_ACCESS_KEY and MINIO_SECRET_KEY "
            "must be defined in .env"
        )

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


# ---------------------------------------------------------
# Check whether bucket exists
# ---------------------------------------------------------

def ensure_bucket(s3_client):
    try:
        s3_client.head_bucket(
            Bucket=MINIO_BUCKET
        )

        print(
            f"Bucket '{MINIO_BUCKET}' already exists."
        )

    except ClientError:
        print(
            f"Bucket '{MINIO_BUCKET}' not found."
        )

        print("Creating bucket...")

        s3_client.create_bucket(
            Bucket=MINIO_BUCKET
        )

        print(
            f"Created bucket '{MINIO_BUCKET}'."
        )


# ---------------------------------------------------------
# Count records
# ---------------------------------------------------------

def count_records(file_path):
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        with open(
            file_path,
            "r",
            encoding="utf-8",
            newline=""
        ) as file:

            reader = csv.DictReader(file)

            return sum(
                1 for _ in reader
            )

    if suffix == ".json":
        with open(
            file_path,
            "r",
            encoding="utf-8"
        ) as file:

            data = json.load(file)

            if isinstance(data, list):
                return len(data)

            return 1

    return None


# ---------------------------------------------------------
# Upload one source file
# ---------------------------------------------------------

def upload_source(
    s3_client,
    source_name,
    file_path,
    ingestion_date
):

    if not file_path.exists():
        raise FileNotFoundError(
            f"Source file not found: {file_path}"
        )

    year = ingestion_date.strftime("%Y")
    month = ingestion_date.strftime("%m")
    day = ingestion_date.strftime("%d")

    object_key = (
        f"{source_name}/"
        f"{year}/"
        f"{month}/"
        f"{day}/"
        f"{file_path.name}"
    )

    # Check if object already exists
    try:
        s3_client.head_object(
            Bucket=MINIO_BUCKET,
            Key=object_key
        )

        print(
            f"SKIPPED: {object_key} "
            f"(already exists)"
        )

        return {
            "source": source_name,
            "file": file_path.name,
            "object_key": object_key,
            "row_count": count_records(file_path),
            "status": "SKIPPED",
        }

    except ClientError as error:
        error_code = error.response.get(
            "Error", {}
        ).get(
            "Code"
        )

        if error_code not in [
            "404",
            "NoSuchKey",
            "NoSuchBucket"
        ]:
            raise

    # Upload original file
    s3_client.upload_file(
        str(file_path),
        MINIO_BUCKET,
        object_key
    )

    row_count = count_records(file_path)

    print(
        f"UPLOADED: {object_key} "
        f"| Records: {row_count}"
    )

    return {
        "source": source_name,
        "file": file_path.name,
        "object_key": object_key,
        "row_count": row_count,
        "status": "UPLOADED",
    }


# ---------------------------------------------------------
# Main ingestion process
# ---------------------------------------------------------

def main():

    print("=" * 70)
    print("RetailLake Raw Data Ingestion")
    print("=" * 70)

    ingestion_time = datetime.now()

    ingestion_date = ingestion_time.date()

    print(
        f"Ingestion time: "
        f"{ingestion_time.strftime('%Y-%m-%d %H:%M:%S')}"
    )

    print(
        f"Target bucket: {MINIO_BUCKET}"
    )

    print()

    # Create MinIO client
    s3_client = create_minio_client()

    # Make sure bucket exists
    ensure_bucket(s3_client)

    print()

    results = []

    # Upload every source
    for source_name, file_path in SOURCE_FILES.items():

        result = upload_source(
            s3_client,
            source_name,
            file_path,
            ingestion_date
        )

        results.append(result)

    # -----------------------------------------------------
    # Create ingestion metadata
    # -----------------------------------------------------

    metadata_key = (
        f"_metadata/"
        f"{ingestion_date.strftime('%Y')}/"
        f"{ingestion_date.strftime('%m')}/"
        f"{ingestion_date.strftime('%d')}/"
        f"ingestion_manifest.json"
    )

    manifest = {
        "platform": "RetailLake",
        "bucket": MINIO_BUCKET,
        "ingestion_runtime": ingestion_time.isoformat(),
        "ingestion_date": ingestion_date.isoformat(),
        "sources": results,
    }

    metadata_file = (
        PROJECT_ROOT /
        "data" /
        "processed" /
        "ingestion_manifest.json"
    )

    metadata_file.parent.mkdir(
        parents=True,
        exist_ok=True
    )

    with open(
        metadata_file,
        "w",
        encoding="utf-8"
    ) as file:

        json.dump(
            manifest,
            file,
            indent=2
        )

    # Upload metadata
    s3_client.upload_file(
        str(metadata_file),
        MINIO_BUCKET,
        metadata_key
    )

    print()
    print(
        f"METADATA: {metadata_key}"
    )

    print()
    print("=" * 70)
    print("Raw ingestion completed successfully.")
    print("=" * 70)


if __name__ == "__main__":
    main()