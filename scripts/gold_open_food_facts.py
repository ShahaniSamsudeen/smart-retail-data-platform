
import io
import os
from datetime import datetime, timezone

import boto3
import pandas as pd
import psycopg2
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

SILVER_BUCKET = "retaillake-silver"

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = int(
    os.getenv(
        "POSTGRES_PORT",
        "5432"
    )
)

POSTGRES_DB = os.getenv(
    "POSTGRES_DB",
    "retaillake"
)

POSTGRES_USER = os.getenv(
    "POSTGRES_USER",
    "retaillake_user"
)

POSTGRES_PASSWORD = os.getenv(
    "POSTGRES_PASSWORD",
    "change_me"
)


# ============================================================
# CONFIGURATION
# ============================================================

SILVER_PREFIX = "api/open_food_facts/"

print()
print("=" * 60)
print("RetailLake Gold Layer - Open Food Facts Enrichment")
print("=" * 60)


# ============================================================
# MINIO CONNECTION
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1",
)


# ============================================================
# FIND LATEST SILVER API FILE
# ============================================================

response = s3.list_objects_v2(
    Bucket=SILVER_BUCKET,
    Prefix=SILVER_PREFIX
)

objects = response.get(
    "Contents",
    []
)

objects = [
    obj
    for obj in objects
    if obj["Key"].endswith(
        "open_food_facts_clean.parquet"
    )
]

if not objects:
    raise FileNotFoundError(
        "No Silver Open Food Facts clean Parquet file found."
    )

latest = max(
    objects,
    key=lambda obj: obj["LastModified"]
)

silver_key = latest["Key"]

print(
    f"Silver API source: {silver_key}"
)


# ============================================================
# DOWNLOAD SILVER API FILE
# ============================================================

response = s3.get_object(
    Bucket=SILVER_BUCKET,
    Key=silver_key
)

df = pd.read_parquet(
    io.BytesIO(
        response["Body"].read()
    )
)

print(
    f"Silver API rows: {len(df)}"
)


# ============================================================
# PREPARE DATA
# ============================================================

expected_columns = [
    "code",
    "product_name",
    "brands",
    "categories",
    "quantity",
    "nutriscore_grade",
    "countries",
]

for column in expected_columns:
    if column not in df.columns:
        df[column] = None

df = df[expected_columns].copy()

# Convert missing values to None for PostgreSQL
df = df.where(
    pd.notna(df),
    None
)


# ============================================================
# CONNECT TO POSTGRESQL
# ============================================================

print()
print("Connecting to PostgreSQL...")

connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
)

cursor = connection.cursor()

print(
    "PostgreSQL connection successful!"
)


# ============================================================
# CREATE GOLD SCHEMA AND ENRICHMENT TABLE
# ============================================================

cursor.execute(
    """
    CREATE SCHEMA IF NOT EXISTS gold;

    CREATE TABLE IF NOT EXISTS gold.dim_product_api_enrichment (

        product_code VARCHAR(100) PRIMARY KEY,

        product_name TEXT,

        brands TEXT,

        categories TEXT,

        quantity TEXT,

        nutriscore_grade VARCHAR(20),

        countries TEXT,

        source VARCHAR(100) NOT NULL,

        processed_at TIMESTAMP WITH TIME ZONE NOT NULL

    );
    """
)

connection.commit()


# ============================================================
# REFRESH API ENRICHMENT DATA
# ============================================================

cursor.execute(
    """
    TRUNCATE TABLE gold.dim_product_api_enrichment;
    """
)

processed_at = datetime.now(
    timezone.utc
)

insert_query = """
    INSERT INTO gold.dim_product_api_enrichment (

        product_code,
        product_name,
        brands,
        categories,
        quantity,
        nutriscore_grade,
        countries,
        source,
        processed_at

    )
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

for _, row in df.iterrows():

    cursor.execute(
        insert_query,
        (
            row["code"],
            row["product_name"],
            row["brands"],
            row["categories"],
            row["quantity"],
            row["nutriscore_grade"],
            row["countries"],
            "OPEN_FOOD_FACTS_API",
            processed_at,
        )
    )

connection.commit()


# ============================================================
# VALIDATE RESULTS
# ============================================================

cursor.execute(
    """
    SELECT COUNT(*)
    FROM gold.dim_product_api_enrichment;
    """
)

row_count = cursor.fetchone()[0]


# ============================================================
# CLOSE CONNECTION
# ============================================================

cursor.close()
connection.close()


# ============================================================
# RESULTS
# ============================================================

print()
print("=" * 60)
print("OPEN FOOD FACTS GOLD RESULTS")
print("=" * 60)

print(
    f"Silver API rows:  {len(df)}"
)

print(
    f"Gold table rows:  {row_count}"
)

print()
print(
    "Gold table created/refreshed successfully:"
)

print(
    "  gold.dim_product_api_enrichment"
)

print()
print(
    "Open Food Facts Gold processing completed successfully."
)