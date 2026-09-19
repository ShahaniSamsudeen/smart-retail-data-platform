from pathlib import Path
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
POS_FILE = PROJECT_ROOT / "data" / "raw" / "pos" / "pos_sales.csv"


def test_raw_data_contains_negative_quantities_for_cleaning():
    data = pd.read_csv(POS_FILE)

    assert (data["quantity"] < 0).any(), (
        "Expected at least one negative quantity in the Raw layer"
    )


def test_raw_data_contains_negative_prices_for_cleaning():
    data = pd.read_csv(POS_FILE)

    assert (data["unit_price"] < 0).any(), (
        "Expected at least one negative price in the Raw layer"
    )


def test_transaction_ids_are_not_empty():
    data = pd.read_csv(POS_FILE)

    assert data["transaction_id"].notna().all(), (
        "Missing transaction IDs were found"
    )

def test_raw_pos_data_contains_records_requiring_quarantine():
    data = pd.read_csv(POS_FILE)

    invalid_records = (
        (data["quantity"] <= 0)
        | (data["unit_price"] <= 0)
        | (data["customer_id"].isna())
    )

    assert invalid_records.any(), (
        "Expected at least one invalid POS record for quarantine testing"
    )