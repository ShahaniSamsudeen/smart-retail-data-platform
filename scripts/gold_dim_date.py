import os
from datetime import date, timedelta

import psycopg2
from dotenv import load_dotenv


# ============================================================
# LOAD ENVIRONMENT VARIABLES
# ============================================================

load_dotenv()

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "localhost")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "retaillake")
POSTGRES_USER = os.getenv("POSTGRES_USER", "retaillake_user")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "change_me")


# ============================================================
# CONFIGURATION
# ============================================================

START_DATE = date(2024, 1, 1)
END_DATE = date(2030, 12, 31)


print()
print("=" * 60)
print("RetailLake Gold Layer - dim_date")
print("=" * 60)
print(f"Date range: {START_DATE} to {END_DATE}")
print()
print("Connecting to PostgreSQL...")


# ============================================================
# POSTGRESQL CONNECTION
# ============================================================

connection = psycopg2.connect(
    host=POSTGRES_HOST,
    port=POSTGRES_PORT,
    database=POSTGRES_DB,
    user=POSTGRES_USER,
    password=POSTGRES_PASSWORD,
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

connection.commit()


# ============================================================
# CREATE DIM_DATE TABLE IF IT DOES NOT EXIST
# ============================================================

cursor.execute(
    """
    CREATE TABLE IF NOT EXISTS gold.dim_date (

        date_key INTEGER PRIMARY KEY,

        full_date DATE NOT NULL UNIQUE,

        day INTEGER NOT NULL,

        month INTEGER NOT NULL,

        month_name VARCHAR(20) NOT NULL,

        quarter INTEGER NOT NULL,

        year INTEGER NOT NULL,

        day_of_week INTEGER NOT NULL,

        day_name VARCHAR(20) NOT NULL,

        is_weekend BOOLEAN NOT NULL
    );
    """
)

connection.commit()


# ============================================================
# GENERATE DATE RECORDS
# ============================================================

date_rows = []

current_date = START_DATE

while current_date <= END_DATE:

    date_key = int(current_date.strftime("%Y%m%d"))

    day = current_date.day
    month = current_date.month
    month_name = current_date.strftime("%B")

    quarter = ((current_date.month - 1) // 3) + 1

    year = current_date.year

    day_of_week = current_date.isoweekday()
    day_name = current_date.strftime("%A")

    is_weekend = current_date.weekday() >= 5

    date_rows.append(
        (
            date_key,
            current_date,
            day,
            month,
            month_name,
            quarter,
            year,
            day_of_week,
            day_name,
            is_weekend,
        )
    )

    current_date += timedelta(days=1)


# ============================================================
# INSERT ONLY MISSING DATES
# ============================================================

inserted_count = 0
existing_count = 0

for row in date_rows:

    cursor.execute(
        """
        SELECT 1
        FROM gold.dim_date
        WHERE date_key = %s
           OR full_date = %s
        LIMIT 1;
        """,
        (row[0], row[1]),
    )

    exists = cursor.fetchone()

    if exists:
        existing_count += 1
        continue

    cursor.execute(
        """
        INSERT INTO gold.dim_date (
            date_key,
            full_date,
            day,
            month,
            month_name,
            quarter,
            year,
            day_of_week,
            day_name,
            is_weekend
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s
        );
        """,
        row,
    )

    inserted_count += 1


connection.commit()


# ============================================================
# VALIDATE ROW COUNT
# ============================================================

cursor.execute(
    """
    SELECT COUNT(*)
    FROM gold.dim_date;
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
print("DIM_DATE PROCESSING RESULTS")
print("=" * 60)

print(f"Expected dates:   {len(date_rows)}")
print(f"Existing dates:   {existing_count}")
print(f"Inserted dates:   {inserted_count}")
print(f"Database rows:    {row_count}")

print()
print("Gold table created/refreshed successfully:")
print("  gold.dim_date")

print()
print("Gold dim_date processing completed successfully.")