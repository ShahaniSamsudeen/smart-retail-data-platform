from pathlib import Path
import random
import csv
import json
from datetime import datetime, timedelta


# Fixed seed so the generated data is repeatable
random.seed(42)


# Project directories
PROJECT_ROOT = Path(__file__).resolve().parents[1]
RAW_DATA_DIR = PROJECT_ROOT / "data" / "raw"


def generate_stores():
    stores = [
        {
            "store_id": "ST001",
            "store_name": "RetailLake Colombo",
            "city": "Colombo",
            "region": "Western",
        },
        {
            "store_id": "ST002",
            "store_name": "RetailLake Kandy",
            "city": "Kandy",
            "region": "Central",
        },
        {
            "store_id": "ST003",
            "store_name": "RetailLake Galle",
            "city": "Galle",
            "region": "Southern",
        },
        {
            "store_id": "ST004",
            "store_name": "RetailLake Badulla",
            "city": "Badulla",
            "region": "Uva",
        },
        {
            "store_id": "ST005",
            "store_name": "RetailLake Kurunegala",
            "city": "Kurunegala",
            "region": "North Western",
        },
        {
            "store_id": "ST006",
            "store_name": "RetailLake Jaffna",
            "city": "Jaffna",
            "region": "Northern",
        },
    ]

    output_dir = RAW_DATA_DIR / "stores"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "stores.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=["store_id", "store_name", "city", "region"],
        )

        writer.writeheader()
        writer.writerows(stores)

    print(f"Created {len(stores)} stores: {output_file}")


def generate_products():
    categories = {
        "Electronics": [
            "Wireless Mouse",
            "Bluetooth Speaker",
            "USB Cable",
            "Power Bank",
            "Keyboard",
            "Headphones",
            "Webcam",
            "Smart Watch",
        ],
        "Home & Kitchen": [
            "Electric Kettle",
            "Rice Cooker",
            "Water Bottle",
            "Dinner Set",
            "Frying Pan",
            "Storage Container",
            "Kitchen Knife",
            "Table Lamp",
        ],
        "Grocery": [
            "Rice",
            "Sugar",
            "Tea",
            "Coffee",
            "Milk Powder",
            "Biscuits",
            "Cooking Oil",
            "Cereal",
        ],
        "Personal Care": [
            "Shampoo",
            "Conditioner",
            "Face Wash",
            "Body Lotion",
            "Toothpaste",
            "Soap",
            "Deodorant",
            "Hand Wash",
        ],
        "Clothing": [
            "T-Shirt",
            "Jeans",
            "Casual Shirt",
            "Dress",
            "Hoodie",
            "Shorts",
            "Skirt",
            "Sports Trousers",
        ],
        "Stationery": [
            "Notebook",
            "Ball Pen",
            "Pencil Set",
            "Marker Set",
            "File Folder",
            "Stapler",
            "Calculator",
            "Sketch Book",
        ],
        "Sports": [
            "Football",
            "Basketball",
            "Tennis Racket",
            "Yoga Mat",
            "Skipping Rope",
            "Water Bottle",
            "Sports Bag",
            "Training Gloves",
        ],
    }

    brands = [
        "Nova",
        "LankaPlus",
        "SmartChoice",
        "Prime",
        "UrbanLife",
        "EverBest",
        "ProMax",
        "DailyMart",
    ]

    products = []

    product_number = 1

    for category, product_names in categories.items():
        for product_name in product_names:
            for variant in range(1, 7):
                unit_cost = round(random.uniform(150, 15000), 2)

                selling_price = round(
                    unit_cost * random.uniform(1.15, 1.60),
                    2,
                )

                products.append(
                    {
                        "product_id": f"PRD{product_number:04d}",
                        "product_name": f"{product_name} {variant}",
                        "category": category,
                        "brand": random.choice(brands),
                        "unit_cost": unit_cost,
                        "selling_price": selling_price,
                        "supplier_id": f"SUP{random.randint(1, 12):03d}",
                    }
                )

                product_number += 1

    output_dir = RAW_DATA_DIR / "products"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "products.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "product_id",
                "product_name",
                "category",
                "brand",
                "unit_cost",
                "selling_price",
                "supplier_id",
            ],
        )

        writer.writeheader()
        writer.writerows(products)

    print(f"Created {len(products)} products: {output_file}")


def generate_customers():
    first_names = [
        "Amal",
        "Nimal",
        "Kamal",
        "Saman",
        "Ruwan",
        "Kasun",
        "Dinesh",
        "Tharindu",
        "Isuru",
        "Chamath",
        "Ayesha",
        "Nadeesha",
        "Dilani",
        "Hiruni",
        "Shalini",
        "Tharushi",
        "Anjali",
        "Fathima",
        "Zainab",
        "Madhavi",
    ]

    last_names = [
        "Perera",
        "Fernando",
        "Silva",
        "Bandara",
        "Jayasinghe",
        "Wijesinghe",
        "Gunawardena",
        "Rathnayake",
        "Senanayake",
        "Karunaratne",
        "De Silva",
        "Rajapaksha",
        "Herath",
        "Abeysekara",
    ]

    cities = [
        "Colombo",
        "Kandy",
        "Galle",
        "Badulla",
        "Kurunegala",
        "Jaffna",
        "Negombo",
        "Matara",
        "Ratnapura",
        "Nuwara Eliya",
    ]

    loyalty_tiers = [
        "Bronze",
        "Silver",
        "Gold",
        "Platinum",
    ]

    customers = []

    for customer_number in range(1, 1051):
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)

        email = (
            f"{first_name.lower()}.{last_name.lower().replace(' ', '')}"
            f"{customer_number}@retaillake.lk"
        )

        # Deliberately create a few invalid emails
        if customer_number in [1001, 1002, 1003, 1004, 1005]:
            email = f"invalid-email-{customer_number}"

        customer = {
            "customer_id": f"CUST{customer_number:04d}",
            "first_name": first_name,
            "last_name": last_name,
            "email": email,
            "phone": f"07{random.randint(10000000, 99999999)}",
            "city": random.choice(cities),
            "loyalty_tier": random.choice(loyalty_tiers),
            "registration_date": (
                f"{random.randint(2021, 2025)}-"
                f"{random.randint(1, 12):02d}-"
                f"{random.randint(1, 28):02d}"
            ),
        }

        customers.append(customer)

    output_dir = RAW_DATA_DIR / "crm"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "customers.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
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

        writer.writeheader()
        writer.writerows(customers)

    print(f"Created {len(customers)} customers: {output_file}")


def generate_pos_sales():
    products_file = RAW_DATA_DIR / "products" / "products.csv"

    products = []

    with open(products_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            products.append(row)

    customers_file = RAW_DATA_DIR / "crm" / "customers.csv"

    customers = []

    with open(customers_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            customers.append(row)

    store_ids = [
        "ST001",
        "ST002",
        "ST003",
        "ST004",
        "ST005",
        "ST006",
    ]

    payment_methods = [
        "Cash",
        "Card",
        "Online",
        "Mobile Wallet",
    ]

    sales = []

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)

    date_range_days = (end_date - start_date).days

    for transaction_number in range(1, 5501):
        transaction_id = f"TXN{transaction_number:06d}"

        transaction_date = start_date + timedelta(
            days=random.randint(0, date_range_days),
            hours=random.randint(8, 21),
            minutes=random.randint(0, 59),
        )

        product = random.choice(products)
        customer = random.choice(customers)

        quantity = random.randint(1, 5)

        unit_price = float(product["selling_price"])

        sale = {
            "transaction_id": transaction_id,
            "transaction_date": transaction_date.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "store_id": random.choice(store_ids),
            "product_id": product["product_id"],
            "customer_id": customer["customer_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "payment_method": random.choice(payment_methods),
            "sales_channel": "Physical Store",
        }

        sales.append(sale)

    # Deliberately introduce data-quality problems

    # Duplicate transaction ID
    sales[100]["transaction_id"] = sales[99]["transaction_id"]

    # Missing customer ID
    sales[200]["customer_id"] = ""

    # Negative quantity
    sales[300]["quantity"] = -3

    # Negative price
    sales[400]["unit_price"] = -500

    # Future-dated transaction
    future_date = datetime(2030, 6, 15, 12, 30, 0)

    sales[500]["transaction_date"] = future_date.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # Unknown product ID
    sales[600]["product_id"] = "PRD9999"

    # Unknown customer ID
    sales[700]["customer_id"] = "CUST9999"

    # Mixed timestamp format
    sales[800]["transaction_date"] = "15/08/2025 14:30"

    output_dir = RAW_DATA_DIR / "pos"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "pos_sales.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
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

        writer.writeheader()
        writer.writerows(sales)

    print(f"Created {len(sales)} POS sales: {output_file}")


def generate_ecommerce_orders():
    products_file = RAW_DATA_DIR / "products" / "products.csv"

    products = []

    with open(products_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            products.append(row)

    customers_file = RAW_DATA_DIR / "crm" / "customers.csv"

    customers = []

    with open(customers_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            customers.append(row)

    payment_methods = [
        "Card",
        "Online Banking",
        "Mobile Wallet",
        "Cash on Delivery",
    ]

    regions = [
        "Western",
        "Central",
        "Southern",
        "Uva",
        "Northern",
        "Eastern",
        "North Western",
        "North Central",
        "Sabaragamuwa",
    ]

    order_statuses = [
        "Completed",
        "Completed",
        "Completed",
        "Shipped",
        "Processing",
    ]

    orders = []

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)

    date_range_days = (end_date - start_date).days

    for order_number in range(1, 3501):
        order_id = f"WEB{order_number:06d}"

        order_date = start_date + timedelta(
            days=random.randint(0, date_range_days),
            hours=random.randint(7, 23),
            minutes=random.randint(0, 59),
        )

        product = random.choice(products)
        customer = random.choice(customers)

        quantity = random.randint(1, 4)

        unit_price = float(product["selling_price"])

        order = {
            "order_id": order_id,
            "order_date": order_date.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            "customer_id": customer["customer_id"],
            "product_id": product["product_id"],
            "quantity": quantity,
            "unit_price": unit_price,
            "payment_method": random.choice(payment_methods),
            "delivery_region": random.choice(regions),
            "order_status": random.choice(order_statuses),
            "sales_channel": "E-commerce",
        }

        orders.append(order)

    # Deliberately introduce data-quality problems

    # Duplicate order ID
    orders[100]["order_id"] = orders[99]["order_id"]

    # Missing customer ID
    orders[200]["customer_id"] = ""

    # Negative quantity
    orders[300]["quantity"] = -2

    # Negative price
    orders[400]["unit_price"] = -1000

    # Future-dated order
    future_date = datetime(2030, 7, 20, 15, 45, 0)

    orders[500]["order_date"] = future_date.strftime(
        "%Y-%m-%d %H:%M:%S"
    )

    # Unknown product ID
    orders[600]["product_id"] = "PRD8888"

    # Unknown customer ID
    orders[700]["customer_id"] = "CUST8888"

    # Mixed timestamp format
    orders[800]["order_date"] = "20/08/2025 16:20"

    output_dir = RAW_DATA_DIR / "ecommerce"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "ecommerce_orders.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(orders, file, indent=2)

    print(f"Created {len(orders)} e-commerce orders: {output_file}")


def generate_inventory():
    # Load products
    products_file = RAW_DATA_DIR / "products" / "products.csv"

    products = []

    with open(products_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            products.append(row)

    store_ids = [
        "ST001",
        "ST002",
        "ST003",
        "ST004",
        "ST005",
        "ST006",
    ]

    inventory_records = []

    start_date = datetime(2025, 1, 1)

    for record_number in range(1, 1001):
        snapshot_date = start_date + timedelta(
            days=random.randint(0, 364)
        )

        product = random.choice(products)
        store_id = random.choice(store_ids)

        stock_quantity = random.randint(0, 250)
        reorder_level = random.randint(10, 50)

        if stock_quantity == 0:
            stock_status = "Out of Stock"
        elif stock_quantity <= reorder_level:
            stock_status = "Low Stock"
        else:
            stock_status = "In Stock"

        inventory_records.append(
            {
                "snapshot_date": snapshot_date.strftime("%Y-%m-%d"),
                "product_id": product["product_id"],
                "store_id": store_id,
                "stock_quantity": stock_quantity,
                "reorder_level": reorder_level,
                "stock_status": stock_status,
            }
        )

    # Deliberately introduce data-quality problems

    # Negative stock quantity
    inventory_records[100]["stock_quantity"] = -10
    inventory_records[100]["stock_status"] = "In Stock"

    # Unknown product ID
    inventory_records[200]["product_id"] = "PRD7777"

    # Unknown store ID
    inventory_records[300]["store_id"] = "ST999"

    # Future inventory snapshot
    inventory_records[400]["snapshot_date"] = "2030-12-01"

    output_dir = RAW_DATA_DIR / "inventory"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "inventory.csv"

    with open(output_file, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(
            file,
            fieldnames=[
                "snapshot_date",
                "product_id",
                "store_id",
                "stock_quantity",
                "reorder_level",
                "stock_status",
            ],
        )

        writer.writeheader()
        writer.writerows(inventory_records)

    print(
        f"Created {len(inventory_records)} inventory records: "
        f"{output_file}"
    )


def generate_supplier_deliveries():
    # Load products
    products_file = RAW_DATA_DIR / "products" / "products.csv"

    products = []

    with open(products_file, "r", encoding="utf-8") as file:
        reader = csv.DictReader(file)

        for row in reader:
            products.append(row)

    supplier_ids = [
        f"SUP{i:03d}"
        for i in range(1, 13)
    ]

    delivery_statuses = [
        "Delivered",
        "Delivered",
        "Delivered",
        "Pending",
        "Delayed",
    ]

    deliveries = []

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)

    date_range_days = (end_date - start_date).days

    for delivery_number in range(1, 1501):
        delivery_id = f"DEL{delivery_number:06d}"

        delivery_date = start_date + timedelta(
            days=random.randint(0, date_range_days)
        )

        expected_date = delivery_date + timedelta(
            days=random.randint(1, 14)
        )

        product = random.choice(products)

        delivery = {
            "delivery_id": delivery_id,
            "supplier_id": random.choice(supplier_ids),
            "product_id": product["product_id"],
            "delivery_date": delivery_date.strftime("%Y-%m-%d"),
            "expected_date": expected_date.strftime("%Y-%m-%d"),
            "quantity": random.randint(10, 500),
            "delivery_status": random.choice(delivery_statuses),
        }

        deliveries.append(delivery)

    # ---------------------------------------------------------
    # Deliberately introduce data-quality problems
    # ---------------------------------------------------------

    # Duplicate delivery ID
    deliveries[100]["delivery_id"] = deliveries[99]["delivery_id"]

    # Negative delivery quantity
    deliveries[200]["quantity"] = -50

    # Unknown supplier ID
    deliveries[300]["supplier_id"] = "SUP999"

    # Unknown product ID
    deliveries[400]["product_id"] = "PRD9999"

    # Delivery date after expected date
    deliveries[500]["delivery_date"] = "2025-12-20"
    deliveries[500]["expected_date"] = "2025-12-10"

    # Future delivery date
    deliveries[600]["delivery_date"] = "2030-08-15"

    # Mixed date format
    deliveries[700]["delivery_date"] = "20/08/2025"

    output_dir = RAW_DATA_DIR / "suppliers"
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / "supplier_deliveries.json"

    with open(output_file, "w", encoding="utf-8") as file:
        json.dump(deliveries, file, indent=2)

    print(
        f"Created {len(deliveries)} supplier deliveries: "
        f"{output_file}"
    )


def main():
    print("RetailLake mock data generator started.")

    generate_stores()
    generate_products()
    generate_customers()
    generate_pos_sales()
    generate_ecommerce_orders()
    generate_inventory()
    generate_supplier_deliveries()


if __name__ == "__main__":
    main()