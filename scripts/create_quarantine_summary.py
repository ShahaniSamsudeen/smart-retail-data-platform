from io import BytesIO
from pathlib import Path
from datetime import datetime

import boto3
import pandas as pd
from botocore.client import Config
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "retaillake_admin"
MINIO_SECRET_KEY = "retaillake_password"
BUCKET_NAME = "retaillake-silver"


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1",
    )


def read_parquet_from_minio(client, object_key):
    response = client.get_object(
        Bucket=BUCKET_NAME,
        Key=object_key,
    )

    return pd.read_parquet(BytesIO(response["Body"].read()))


def main():
    client = get_minio_client()

    response = client.list_objects_v2(Bucket=BUCKET_NAME)

    objects = response.get("Contents", [])

    quarantine_objects = [
        obj["Key"]
        for obj in objects
        if "quarantine/" in obj["Key"]
        and obj["Key"].endswith(".parquet")
    ]

    if not quarantine_objects:
        print("No quarantine Parquet files were found in MinIO.")
        return

    summary_rows = []

    for object_key in quarantine_objects:
        try:
            dataframe = read_parquet_from_minio(client, object_key)

            dataset_name = object_key.split("/")[-2]

            if "_error_reason" in dataframe.columns:
                reason_counts = (
                    dataframe["_error_reason"]
                    .fillna("Unknown reason")
                    .astype(str)
                    .value_counts()
                )

                for reason, count in reason_counts.items():
                    summary_rows.append(
                        {
                            "dataset": dataset_name,
                            "quarantine_file": object_key,
                            "error_reason": reason,
                            "record_count": int(count),
                        }
                    )
            else:
                summary_rows.append(
                    {
                        "dataset": dataset_name,
                        "quarantine_file": object_key,
                        "error_reason": "Reason not available",
                        "record_count": len(dataframe),
                    }
                )

        except Exception as error:
            print(f"Could not process {object_key}: {error}")

    if not summary_rows:
        print("No quarantine records could be summarized.")
        return

    summary = pd.DataFrame(summary_rows)

    output_directory = PROJECT_ROOT / "data" / "processed"
    output_directory.mkdir(parents=True, exist_ok=True)

    output_file = output_directory / "combined_quarantine_summary.csv"
    summary.to_csv(output_file, index=False)

    print("\nCombined Quarantine Summary")
    print("=" * 40)
    print(summary.to_string(index=False))
    print("\nTotal quarantined records:", summary["record_count"].sum())
    print("Summary saved to:", output_file)
    print("Generated at:", datetime.now())


if __name__ == "__main__":
    main()