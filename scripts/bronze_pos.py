from pathlib import Path
from datetime import datetime
import os
import tempfile

import boto3
import pandas as pd
from botocore.exceptions import ClientError
from dotenv import load_dotenv


# ---------------------------------------------------------
# Load environment variables
# ---------------------------------------------------------

load_dotenv()


# ---------------------------------------------------------
# Project configuration
# ---------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[1]

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

RAW_BUCKET = os.getenv(
    "MINIO_BUCKET",
    "retaillake-raw"
)

BRONZE_BUCKET = "retaillake-bronze"


# ---------------------------------------------------------
# Create MinIO client
# ---------------------------------------------------------

def create_minio_client():

    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        region_name="us-east-1",
    )


# ---------------------------------------------------------
# Find latest POS raw file
# ---------------------------------------------------------

def find_latest_pos_file(s3_client):

    response = s3_client.list_objects_v2(
        Bucket=RAW_BUCKET,
        Prefix="pos/"
    )

    objects = response.get("Contents", [])

    if not objects:
        raise FileNotFoundError(
            "No POS files found in the Raw bucket."
        )

    pos_objects = [
        obj for obj in objects
        if obj["Key"].endswith(".csv")
    ]

    if not pos_objects:
        raise FileNotFoundError(
            "No POS CSV file found in the Raw bucket."
        )

    latest_object = max(
        pos_objects,
        key=lambda obj: obj["LastModified"]
    )

    return latest_object["Key"]


# ---------------------------------------------------------
# Download file from MinIO
# ---------------------------------------------------------

def download_raw_file(
    s3_client,
    object_key,
    local_path
):

    s3_client.download_file(
        RAW_BUCKET,
        object_key,
        str(local_path)
    )


# ---------------------------------------------------------
# Transform POS data
# ---------------------------------------------------------

def transform_pos_data(df):

    print("\nOriginal data types:")
    print(df.dtypes)

    # Convert transaction date to datetime
    df["transaction_date"] = pd.to_datetime(
        df["transaction_date"],
        errors="coerce",
        dayfirst=True
    )

    # Convert quantity to numeric integer
    df["quantity"] = pd.to_numeric(
        df["quantity"],
        errors="coerce"
    )

    # Convert unit price to numeric
    df["unit_price"] = pd.to_numeric(
        df["unit_price"],
        errors="coerce"
    )

    # Standardize missing customer IDs
    df["customer_id"] = (
        df["customer_id"]
        .replace("", pd.NA)
    )

    # Add calculated sales amount
    df["sales_amount"] = (
        df["quantity"] * df["unit_price"]
    )

    # Add audit columns
    df["_source"] = "POS"
    df["_ingested_at"] = datetime.now().isoformat()

    print("\nTransformed data types:")
    print(df.dtypes)

    return df


# ---------------------------------------------------------
# Upload Bronze Parquet
# ---------------------------------------------------------

def upload_bronze_file(
    s3_client,
    local_path,
    object_key
):

    s3_client.upload_file(
        str(local_path),
        BRONZE_BUCKET,
        object_key
    )

    print(
        f"\nUploaded Bronze file: {object_key}"
    )


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def main():

    print("=" * 70)
    print("RetailLake Bronze Layer - POS Sales")
    print("=" * 70)

    s3_client = create_minio_client()

    # Find Raw POS file
    raw_object_key = find_latest_pos_file(
        s3_client
    )

    print(
        f"\nRaw source: {raw_object_key}"
    )

    # Temporary local file
    with tempfile.TemporaryDirectory() as temp_dir:

        temp_dir = Path(temp_dir)

        raw_local_file = (
            temp_dir / "pos_sales.csv"
        )

        bronze_local_file = (
            temp_dir / "pos_sales.parquet"
        )

        # Download Raw file
        print(
            "\nDownloading Raw POS file..."
        )

        download_raw_file(
            s3_client,
            raw_object_key,
            raw_local_file
        )

        # Read CSV
        print(
            "Reading POS CSV..."
        )

        df = pd.read_csv(
            raw_local_file
        )

        print(
            f"Raw rows: {len(df)}"
        )

        # Transform
        df = transform_pos_data(df)

        print(
            f"Bronze rows: {len(df)}"
        )

        # Save Parquet
        df.to_parquet(
            bronze_local_file,
            index=False
        )

        print(
            "\nSaved Bronze Parquet file."
        )

        # Bronze destination
        ingestion_date = datetime.now()

        bronze_object_key = (
            "pos/"
            f"{ingestion_date.strftime('%Y')}/"
            f"{ingestion_date.strftime('%m')}/"
            f"{ingestion_date.strftime('%d')}/"
            "pos_sales.parquet"
        )

        # Upload
        upload_bronze_file(
            s3_client,
            bronze_local_file,
            bronze_object_key
        )

    print("\n" + "=" * 70)
    print("Bronze POS processing completed successfully.")
    print("=" * 70)


if __name__ == "__main__":
    main()