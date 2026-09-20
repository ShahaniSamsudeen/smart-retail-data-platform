from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text


PROJECT_ROOT = Path(__file__).resolve().parent.parent
SUMMARY_FILE = (
    PROJECT_ROOT / "data" / "processed" / "combined_quarantine_summary.csv"
)

DATABASE_URL = (
    "postgresql+psycopg2://retaillake_user:change_me"
    "@localhost:5432/retaillake"
)


def main():
    if not SUMMARY_FILE.exists():
        raise FileNotFoundError(
            f"Summary file not found: {SUMMARY_FILE}"
        )

    dataframe = pd.read_csv(SUMMARY_FILE)

    engine = create_engine(DATABASE_URL)

    with engine.begin() as connection:
        connection.execute(
            text("CREATE SCHEMA IF NOT EXISTS gold")
        )

    dataframe.to_sql(
        "quarantine_summary",
        engine,
        schema="gold",
        if_exists="replace",
        index=False,
    )

    print("Quarantine summary loaded successfully.")
    print(f"Records loaded: {len(dataframe)}")
    print("Table: gold.quarantine_summary")


if __name__ == "__main__":
    main()