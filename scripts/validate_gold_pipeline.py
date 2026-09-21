import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host=os.getenv("POSTGRES_HOST"),
    port=os.getenv("POSTGRES_PORT"),
    dbname=os.getenv("POSTGRES_DB"),
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD")
)

cur = conn.cursor()

print("=" * 60)
print("RetailLake - Complete Gold Layer Validation Report")
print("=" * 60)

checks = {
    "Sales transactions": """
        SELECT COUNT(*) FROM gold.fact_sales_transaction
    """,
    "Inventory records": """
        SELECT COUNT(*) FROM gold.fact_inventory_snapshot
    """,
    "Products": """
        SELECT COUNT(*) FROM gold.dim_product
    """,
    "Customers": """
        SELECT COUNT(*) FROM gold.dim_customer
    """,
    "Stores": """
        SELECT COUNT(*) FROM gold.dim_store
    """,
    "Date records": """
        SELECT COUNT(*) FROM gold.dim_date
    """,
    "Open Food Facts records": """
        SELECT COUNT(*) FROM gold.dim_product_api_enrichment
    """,
    "DummyJSON records": """
        SELECT COUNT(*) FROM gold.dim_product_dummyjson
    """,
    "Quarantine records": """
        SELECT COUNT(*) FROM gold.quarantine_summary
    """,
    "Total revenue": """
        SELECT COALESCE(SUM(net_sales), 0)
        FROM gold.fact_sales_transaction
    """,
    "Duplicate transaction IDs": """
        SELECT COUNT(*) - COUNT(DISTINCT transaction_id)
        FROM gold.fact_sales_transaction
    """,
    "Invalid quantities": """
        SELECT COUNT(*)
        FROM gold.fact_sales_transaction
        WHERE quantity <= 0
    """,
    "Invalid unit prices": """
        SELECT COUNT(*)
        FROM gold.fact_sales_transaction
        WHERE unit_price <= 0
    """,
    "Incorrect sales totals": """
        SELECT COUNT(*)
        FROM gold.fact_sales_transaction
        WHERE net_sales <> quantity * unit_price
    """,
    "Invalid product keys": """
        SELECT COUNT(*)
        FROM gold.fact_sales_transaction f
        LEFT JOIN gold.dim_product p
        ON f.product_key = p.product_key
        WHERE p.product_key IS NULL
    """,
    "Invalid customer keys": """
        SELECT COUNT(*)
        FROM gold.fact_sales_transaction f
        LEFT JOIN gold.dim_customer c
        ON f.customer_key = c.customer_key
        WHERE c.customer_key IS NULL
    """,
    "Invalid store keys": """
        SELECT COUNT(*)
        FROM gold.fact_sales_transaction f
        LEFT JOIN gold.dim_store s
        ON f.store_key = s.store_key
        WHERE s.store_key IS NULL
    """,
    "Invalid date keys": """
        SELECT COUNT(*)
        FROM gold.fact_sales_transaction f
        LEFT JOIN gold.dim_date d
        ON f.date_key = d.date_key
        WHERE d.date_key IS NULL
    """
}

for name, query in checks.items():
    cur.execute(query)
    result = cur.fetchone()[0]
    print(f"{name}: {result}")

print("\nDashboard view check:")

try:
    cur.execute("SELECT COUNT(*) FROM gold.vw_sales_dashboard")
    print("Dashboard view: WORKING")
    print("Dashboard rows:", cur.fetchone()[0])
except Exception as error:
    print("Dashboard view: ERROR")
    print(error)

cur.close()
conn.close()

print("\nValidation completed.")
