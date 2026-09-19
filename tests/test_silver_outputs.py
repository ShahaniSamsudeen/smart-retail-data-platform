import io
from pathlib import Path

import boto3
import pandas as pd


MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "retaillake_admin"
MINIO_SECRET_KEY = "retaillake_password"
SILVER_BUCKET = "retaillake-silver"


s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
)


def get_latest_file(prefix):
    response = s3.list_objects_v2(
        Bucket=SILVER_BUCKET,
        Prefix=prefix,
    )

    objects = [
        obj
        for obj in response.get("Contents", [])
        if obj["Key"].endswith(".parquet")
    ]

    assert objects, f"No Parquet files found under {prefix}"

    return max(objects, key=lambda obj: obj["LastModified"])["Key"]


def read_parquet_from_minio(object_key):
    response = s3.get_object(
        Bucket=SILVER_BUCKET,
        Key=object_key,
    )

    return pd.read_parquet(io.BytesIO(response["Body"].read()))


def test_pos_silver_clean_file_exists():
    clean_key = get_latest_file("pos/")
    clean_data = read_parquet_from_minio(clean_key)

    assert len(clean_data) > 0, "POS Silver clean file is empty"


def test_pos_silver_quarantine_file_exists():
    quarantine_key = get_latest_file("quarantine/pos/")
    quarantine_data = read_parquet_from_minio(quarantine_key)

    assert len(quarantine_data) > 0, (
        "POS Silver quarantine file is empty"
    )
