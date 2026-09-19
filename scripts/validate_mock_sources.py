from pathlib import Path
import csv
import json


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def check_csv(file_path, expected_rows, expected_columns):
    print(f"\nChecking: {file_path.name}")

    with open(file_path, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    actual_rows = len(rows)

    print(f"Rows: {actual_rows}")

    if actual_rows == expected_rows:
        print("PASS: Row count is correct")
    else:
        print(
            f"WARNING: Expected {expected_rows}, "
            f"but found {actual_rows}"
        )

    actual_columns = reader.fieldnames

    if actual_columns == expected_columns:
        print("PASS: Columns are correct")
    else:
        print("WARNING: Columns do not match")
        print("Expected:", expected_columns)
        print("Found:", actual_columns)


def check_json(file_path, expected_rows, expected_columns):
    print(f"\nChecking: {file_path.name}")

    with open(file_path, "r", encoding="utf-8") as file:
        data = json.load(file)

    actual_rows = len(data)

    print(f"Records: {actual_rows}")

    if actual_rows == expected_rows:
        print("PASS: Record count is correct")
    else:
        print(
            f"WARNING: Expected {expected_rows}, "
            f"but found {actual_rows}"
        )

    actual_columns = list(data[0].keys())

    if actual_columns == expected_columns:
        print("PASS: Fields are correct")
    else:
        print("WARNING: Fields do not match")
        print("Expected:", expected_columns)
        print("Found:", actual_columns)


def main():

    print("=" * 60)
    print("RetailLake Mock Source Validation")
    print("=" * 60)

    check_csv(
        RAW_DATA_DIR / "stores" / "stores.csv",
        6,
        [
            "store_id",
            "store_name",
            "city",
            "region",
        ],
    )

    check_csv(
        RAW_DATA_DIR / "products" / "products.csv",
        336,
        [
            "product_id",
            "product_name",
            "category",
            "brand",
            "unit_cost",
            "selling_price",
            "supplier_id",
        ],
    )

    check_csv(
        RAW_DATA_DIR / "crm" / "customers.csv",
        1050,
        [
            "customer_id",
            "first_name",
            "last_name",
            "email",
            "phone",
            "city",
            "loyalty_tier",
            "registration_date",
        ],
    )

    check_csv(
        RAW_DATA_DIR / "pos" / "pos_sales.csv",
        5500,
        [
            "transaction_id",
            "transaction_date",
            "store_id",
            "product_id",
            "customer_id",
            "quantity",
            "unit_price",
            "payment_method",
            "sales_channel",
        ],
    )

    check_json(
        RAW_DATA_DIR / "ecommerce" / "ecommerce_orders.json",
        3500,
        [
            "order_id",
            "order_date",
            "customer_id",
            "product_id",
            "quantity",
            "unit_price",
            "payment_method",
            "delivery_region",
            "order_status",
            "sales_channel",
        ],
    )

    check_csv(
        RAW_DATA_DIR / "inventory" / "inventory.csv",
        1000,
        [
            "snapshot_date",
            "product_id",
            "store_id",
            "stock_quantity",
            "reorder_level",
            "stock_status",
        ],
    )

    check_json(
        RAW_DATA_DIR / "suppliers" / "supplier_deliveries.json",
        1500,
        [
            "delivery_id",
            "supplier_id",
            "product_id",
            "delivery_date",
            "expected_date",
            "quantity",
            "delivery_status",
        ],
    )

    print("\n" + "=" * 60)
    print("Validation completed.")
    print("=" * 60)


if __name__ == "__main__":
    main()