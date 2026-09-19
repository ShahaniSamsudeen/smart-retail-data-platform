
from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="retaillake_pipeline",
    start_date=datetime(2026, 9, 15),
    schedule=None,
    catchup=False,
    tags=["RetailLake", "data-engineering"],
) as dag:

    # =========================================================
    # SOURCE AND INGESTION TASKS
    # =========================================================

    @task
    def run_seed():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/seed_mock_sources.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_api_ingestion():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/ingest_open_food_facts.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_dummyjson_ingestion():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/ingest_dummyjson.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_raw_ingestion():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/ingest_raw.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_bronze():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/bronze_all.py"],
            cwd="/opt/airflow",
            check=True,
        )

    # =========================================================
    # SILVER LAYER TASKS
    # =========================================================

    @task
    def run_silver_pos():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/silver_pos.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_silver_customers():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/silver_customers.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_silver_ecommerce():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/silver_ecommerce.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_silver_products():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/silver_products.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_silver_stores():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/silver_stores.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_silver_inventory():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/silver_inventory.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_silver_suppliers():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/silver_suppliers.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_silver_open_food_facts():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/silver_open_food_facts.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_silver_dummyjson():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/silver_dummyjson.py"],
            cwd="/opt/airflow",
            check=True,
        )

    # =========================================================
    # GOLD LAYER TASKS
    # =========================================================

    @task
    def run_gold_product():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/gold_dim_product.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_gold_customer():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/gold_dim_customer.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_gold_store():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/gold_dim_store.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_gold_date():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/gold_dim_date.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_gold_sales():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/gold_fact_sales.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_gold_inventory():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/gold_fact_inventory.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_gold_open_food_facts():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/gold_open_food_facts.py"],
            cwd="/opt/airflow",
            check=True,
        )

    @task
    def run_gold_dummyjson():
        import subprocess

        subprocess.run(
            ["python", "/opt/airflow/scripts/gold_dummyjson.py"],
            cwd="/opt/airflow",
            check=True,
        )

    # =========================================================
    # CREATE TASKS
    # =========================================================

    seed = run_seed()
    api_ingestion = run_api_ingestion()
    dummyjson_ingestion = run_dummyjson_ingestion()
    raw = run_raw_ingestion()
    bronze = run_bronze()

    silver_pos = run_silver_pos()
    silver_customers = run_silver_customers()
    silver_ecommerce = run_silver_ecommerce()
    silver_products = run_silver_products()
    silver_stores = run_silver_stores()
    silver_inventory = run_silver_inventory()
    silver_suppliers = run_silver_suppliers()
    silver_open_food_facts = run_silver_open_food_facts()
    silver_dummyjson = run_silver_dummyjson()

    gold_product = run_gold_product()
    gold_customer = run_gold_customer()
    gold_store = run_gold_store()
    gold_date = run_gold_date()
    gold_sales = run_gold_sales()
    gold_inventory = run_gold_inventory()
    gold_open_food_facts = run_gold_open_food_facts()
    gold_dummyjson = run_gold_dummyjson()

    # =========================================================
    # PIPELINE DEPENDENCIES
    # =========================================================

    # Seed → API ingestion → DummyJSON ingestion → Raw → Bronze
    seed >> api_ingestion >> dummyjson_ingestion >> raw >> bronze

    # Bronze → Silver
    bronze >> silver_pos
    bronze >> silver_customers
    bronze >> silver_ecommerce
    bronze >> silver_products
    bronze >> silver_stores
    bronze >> silver_inventory
    bronze >> silver_suppliers
    bronze >> silver_open_food_facts
    bronze >> silver_dummyjson

    # Silver → Gold dimensions
    silver_products >> gold_product
    silver_customers >> gold_customer
    silver_stores >> gold_store

    # Gold sales fact
    gold_product >> gold_sales
    gold_customer >> gold_sales
    gold_store >> gold_sales
    gold_date >> gold_sales
    silver_pos >> gold_sales
    silver_ecommerce >> gold_sales

    # Gold inventory fact
    gold_product >> gold_inventory
    gold_store >> gold_inventory
    gold_date >> gold_inventory
    silver_inventory >> gold_inventory

    # Open Food Facts API → Gold enrichment table
    silver_open_food_facts >> gold_open_food_facts

    # DummyJSON API → Gold enrichment table
    silver_dummyjson >> gold_dummyjson