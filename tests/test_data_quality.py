from pathlib import Path
import json
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = PROJECT_ROOT / "data" / "raw"


def test_pos_has_minimum_records():
    file_path = RAW_DIR / "pos" / "pos_sales.csv"
    data = pd.read_csv(file_path)

    assert len(data) >= 5000, "POS data must contain at least 5,000 records"


def test_ecommerce_has_minimum_records():
    file_path = RAW_DIR / "ecommerce" / "ecommerce_orders.json"

    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    assert len(data) >= 3000, (
        "E-commerce data must contain at least 3,000 records"
    )


def test_customers_have_minimum_records():
    file_path = RAW_DIR / "crm" / "customers.csv"
    data = pd.read_csv(file_path)

    assert len(data) >= 1000, (
        "Customer data must contain at least 1,000 records"
    )


def test_products_have_minimum_records():
    file_path = RAW_DIR / "products" / "products.csv"
    data = pd.read_csv(file_path)

    assert len(data) >= 300, (
        "Product data must contain at least 300 records"
    )


def test_inventory_has_minimum_records():
    file_path = RAW_DIR / "inventory" / "inventory.csv"
    data = pd.read_csv(file_path)

    assert len(data) >= 500, (
        "Inventory data must contain at least 500 records"
    )