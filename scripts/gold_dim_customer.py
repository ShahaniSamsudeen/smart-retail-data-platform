import os
import hashlib
from io import BytesIO

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

POSTGRES_HOST = os.getenv(
    "POSTGRES_HOST",
    "localhost"
)

POSTGRES_PORT = os.getenv(
    "POSTGRES_PORT",
    "5432"
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

SILVER_BUCKET = "retaillake-silver"


# ============================================================
# CONNECT TO MINIO
# ============================================================

s3 = boto3.client(
    "s3",
    endpoint_url=MINIO_ENDPOINT,
    aws_access_key_id=MINIO_ACCESS_KEY,
    aws_secret_access_key=MINIO_SECRET_KEY,
    region_name="us-east-1"
)


# ============================================================
# FIND LATEST SILVER CUSTOMER FILE
# ============================================================

response = s3.list_objects_v2(
    Bucket=SILVER_BUCKET,
    Prefix="crm/"
)

objects = [
    obj
    for obj in response.get("Contents", [])
    if obj["Key"].endswith("customers_clean.parquet")
]

if not objects:
    raise FileNotFoundError(
        "No Silver customer parquet file found."
    )

latest_object = max(
    objects,
    key=lambda x: x["LastModified"]
)

silver_key = latest_object["Key"]

print()
print("=" * 60)
print("RetailLake Gold Layer - dim_customer")
print("=" * 60)

print(f"Silver source: {silver_key}")


# ============================================================
# DOWNLOAD SILVER DATA
# ============================================================

obj = s3.get_object(
    Bucket=SILVER_BUCKET,
    Key=silver_key
)

dim_customer = pd.read_parquet(
    BytesIO(obj["Body"].read())
)

print(
    f"Silver customer rows: {len(dim_customer)}"
)

print()
print("Silver customer columns:")
print(dim_customer.columns.tolist())


# ============================================================
# PREPARE CUSTOMER DATA
# ============================================================

dim_customer = dim_customer.copy()

# Generate a stable surrogate key.
dim_customer["customer_key"] = range(
    1,
    len(dim_customer) + 1
)

# Make sure registration date is a proper date.
dim_customer["registration_date"] = pd.to_datetime(
    dim_customer["registration_date"],
    errors="coerce"
).dt.date

# Make sure email hash is stored as text.
dim_customer["email_hash"] = (
    dim_customer["email_hash"]
    .astype("string")
    .replace({"<NA>": None})
)

# Customer email itself should NOT exist in Gold.
if "email" in dim_customer.columns:
    dim_customer = dim_customer.drop(
        columns=["email"]
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
    password=POSTGRES_PASSWORD
)

cursor = connection.cursor()

print("PostgreSQL connection successful!")


# ============================================================
# CREATE GOLD SCHEMA
# ============================================================

cursor.execute(
    """
    CREATE SCHEMA IF NOT EXISTS gold;
    """
)


# ============================================================
# CREATE DIM_CUSTOMER TABLE IF IT DOES NOT EXIST
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS gold.dim_customer (

        customer_key INTEGER PRIMARY KEY,

        customer_id VARCHAR(50) NOT NULL UNIQUE,

        first_name VARCHAR(100),

        last_name VARCHAR(100),

        email_hash VARCHAR(64),

        phone VARCHAR(50),

        city VARCHAR(100),

        loyalty_tier VARCHAR(50),

        registration_date DATE

    );
    """
)


# ============================================================
# INSERT / UPDATE CUSTOMERS
# ============================================================

insert_query = """
    INSERT INTO gold.dim_customer (
        customer_key,
        customer_id,
        first_name,
        last_name,
        email_hash,
        phone,
        city,
        loyalty_tier,
        registration_date
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (customer_id)
    DO UPDATE SET

        first_name = EXCLUDED.first_name,

        last_name = EXCLUDED.last_name,

        email_hash = EXCLUDED.email_hash,

        phone = EXCLUDED.phone,

        city = EXCLUDED.city,

        loyalty_tier = EXCLUDED.loyalty_tier,

        registration_date = EXCLUDED.registration_date;
"""


inserted_or_updated = 0

for _, row in dim_customer.iterrows():

    cursor.execute(
        insert_query,
        (
            int(row["customer_key"]),
            str(row["customer_id"]),
            row["first_name"],
            row["last_name"],
            row["email_hash"],
            row["phone"],
            row["city"],
            row["loyalty_tier"],
            row["registration_date"],
        )
    )

    inserted_or_updated += 1


# ============================================================
# COMMIT
# ============================================================

connection.commit()


# ============================================================
# VERIFY TABLE
# ============================================================

cursor.execute(
    """
    SELECT COUNT(*)
    FROM gold.dim_customer;
    """
)

row_count = cursor.fetchone()[0]

print()
print(f"Processed customers: {inserted_or_updated}")
print(f"Database rows: {row_count}")


# ============================================================
# PII VERIFICATION
# ============================================================

cursor.execute(
    """
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = 'gold'
      AND table_name = 'dim_customer'
    ORDER BY ordinal_position;
    """
)

columns = [
    row[0]
    for row in cursor.fetchall()
]

print()
print("Gold dim_customer columns:")
print(columns)

if "email" not in columns:
    print("PII protection: Original email is NOT stored in Gold.")
else:
    print("WARNING: Original email column exists!")


# ============================================================
# CLOSE CONNECTION
# ============================================================

cursor.close()
connection.close()


print()
print("=" * 60)
print("Gold table created/refreshed successfully:")
print("gold.dim_customer")
print("=" * 60)