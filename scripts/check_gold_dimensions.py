from dotenv import load_dotenv
import os
from sqlalchemy import create_engine, text

load_dotenv()

url = (
    f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:"
    f"{os.getenv('POSTGRES_PASSWORD')}@"
    f"{os.getenv('POSTGRES_HOST')}:"
    f"{os.getenv('POSTGRES_PORT')}/"
    f"{os.getenv('POSTGRES_DB')}"
)

engine = create_engine(url)

tables = [
    "dim_date",
    "dim_product",
    "dim_customer",
    "dim_store"
]

with engine.connect() as connection:
    for table in tables:
        print(f"\n--- gold.{table} ---")
        result = connection.execute(text(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'gold'
            AND table_name = '{table}'
            ORDER BY ordinal_position
        """))

        for row in result:
            print(row)
