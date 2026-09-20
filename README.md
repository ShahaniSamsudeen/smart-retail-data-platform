# RetailLake Smart Retail Data Platform

A local smart retail data platform built using a modern data engineering pipeline.

The platform generates retail data, ingests data from multiple sources and APIs, processes it through Raw, Bronze, Silver, and Gold layers, validates data quality, and presents business insights through Apache Superset.

## Project Objectives

- Generate realistic retail mock data.
- Ingest CSV and JSON data into MinIO.
- Integrate external APIs into the data pipeline.
- Process data through the Medallion architecture.
- Clean and validate data.
- Quarantine invalid records with error reasons.
- Build Gold fact and dimension tables in PostgreSQL.
- Orchestrate the pipeline using Apache Airflow.
- Create business dashboards using Apache Superset.
- Perform automated testing using Pytest.
- Maintain the project using Git and GitHub.

## Architecture

```text
Mock Data Sources and External APIs
                |
                v
        Apache Airflow
                |
                v
       MinIO - Raw Layer
                |
                v
          Bronze Layer
                |
                v
          Silver Layer
          /         \
         v           v
    Clean Data   Quarantine Data
         |
         v
   PostgreSQL - Gold Layer
         |
         v
 Apache Superset Dashboard
```

## Data Sources

The platform uses the following data sources:

- Point-of-sale transactions
- E-commerce orders
- Customer records
- Product records
- Store records
- Inventory records
- Supplier deliveries
- Open Food Facts API
- DummyJSON API

## Technology Stack

| Component | Technology |
|---|---|
| Programming Language | Python 3.12 |
| Orchestration | Apache Airflow |
| Object Storage | MinIO |
| Database | PostgreSQL |
| Data Processing | Pandas and PyArrow |
| Dashboard | Apache Superset |
| Containerisation | Docker Compose |
| Testing | Pytest |
| Version Control | Git and GitHub |
| External APIs | Open Food Facts and DummyJSON |

## Medallion Architecture

### Raw Layer

The Raw layer stores original source files and API data in MinIO.

Data is organised using date-based folder structures to support data tracking and ingestion auditing.

### Bronze Layer

The Bronze layer converts source files into structured and typed Parquet files while preserving important ingestion information.

### Silver Layer

The Silver layer cleans and validates the data.

The following data-quality operations are performed:

- Data-type conversion
- Missing-value checks
- Duplicate detection
- Invalid price checks
- Invalid quantity checks
- Date validation
- Identifier validation
- Email validation
- Data standardisation
- Separation of invalid records

Invalid records are stored in quarantine outputs together with error reasons.

Customer email addresses are hashed using the SHA-256 algorithm before being used in the trusted and reporting layers.

### Gold Layer

The Gold layer contains reporting-ready fact and dimension tables in PostgreSQL.

The Gold tables are used to support business reporting and dashboard development.

### Business Intelligence Layer

Apache Superset connects to the Gold-layer data and provides interactive dashboards for sales, inventory, data quality, and API-enrichment analysis.

## Gold Tables

The main Gold tables include:

- `gold.dim_customer`
- `gold.dim_product`
- `gold.dim_store`
- `gold.dim_date`
- `gold.fact_sales_transaction`
- `gold.fact_inventory_snapshot`
- `gold.dim_product_api_enrichment`
- `gold.dim_product_dummyjson`
- `gold.quarantine_summary`

The project also provides the following reporting view:

- `gold.vw_sales_dashboard`

## Dashboard Charts

The RetailLake Smart Retail Dashboard includes the following visualisations:

- Daily sales revenue trend
- Revenue by sales channel
- Top 10 products by net sales
- Low-stock and out-of-stock products
- Data-quality summary
- Open Food Facts product information
- DummyJSON product information

The dashboard uses Gold-layer data stored in PostgreSQL.

## API Integrations

### Open Food Facts API

The Open Food Facts API provides product and food-related information.

The API data is processed through the following layers:

```text
Open Food Facts API
        |
        v
       Raw
        |
        v
      Bronze
        |
        v
      Silver
        |
        v
       Gold
```

The processed information is stored in:

`gold.dim_product_api_enrichment`

### DummyJSON API

The DummyJSON API provides product information for data-enrichment and demonstration purposes.

The API data is processed through the Raw, Bronze, Silver, and Gold layers.

The processed information is stored in:

`gold.dim_product_dummyjson`

## Project Structure

```text
smart-retail-data-platform/
|
|-- .github/
|   `-- workflows/
|       `-- tests.yml
|
|-- data/
|   |-- raw/
|   `-- processed/
|
|-- dbt_smart_retail/
|   `-- models/
|
|-- docker/
|   |-- airflow/
|   |   `-- dags/
|   |       `-- retaillake_pipeline.py
|   `-- docker-compose.yml
|
|-- scripts/
|   |-- seed_mock_sources.py
|   |-- validate_mock_sources.py
|   |-- ingest_raw.py
|   |-- ingest_open_food_facts.py
|   |-- ingest_dummyjson.py
|   |-- bronze_all.py
|   |-- silver_pos.py
|   |-- silver_ecommerce.py
|   |-- silver_customers.py
|   |-- silver_products.py
|   |-- silver_stores.py
|   |-- silver_inventory.py
|   |-- silver_suppliers.py
|   |-- silver_open_food_facts.py
|   |-- silver_dummyjson.py
|   |-- gold_dim_customer.py
|   |-- gold_dim_product.py
|   |-- gold_dim_store.py
|   |-- gold_dim_date.py
|   |-- gold_fact_sales.py
|   |-- gold_fact_inventory.py
|   |-- gold_open_food_facts.py
|   |-- gold_dummyjson.py
|   |-- create_quarantine_summary.py
|   `-- gold_quarantine_summary.py
|
|-- src/
|
|-- tests/
|   |-- test_project_structure.py
|   |-- test_data_quality.py
|   |-- test_data_quality_rules.py
|   `-- test_silver_outputs.py
|
|-- .env.example
|-- README.md
`-- requirements.txt
```

## Prerequisites

Before running the project, install the following software:

- Python 3.12
- Docker Desktop
- Git
- Visual Studio Code

Docker Desktop must be running before starting the platform services.

## Setup Instructions

### 1. Clone the Repository

```powershell
git clone https://github.com/ShahaniSamsudeen/smart-retail-data-platform.git
```

Move into the project directory:

```powershell
cd smart-retail-data-platform
```

### 2. Create a Virtual Environment

```powershell
python -m venv .venv
```

### 3. Activate the Virtual Environment on Windows

```powershell
.venv\Scripts\Activate.ps1
```

### 4. Install Dependencies

```powershell
pip install -r requirements.txt
```

Install Pytest if it is not already included in the environment:

```powershell
pip install pytest
```

### 5. Configure Environment Variables

Create a local `.env` file based on `.env.example`.

```powershell
Copy-Item .env.example .env
```

Do not upload the `.env` file to GitHub.

Do not include passwords, access keys, or other sensitive credentials in the repository.

## Starting the Services

Make sure Docker Desktop is running.

From the project root directory, execute:

```powershell
docker compose -f docker\docker-compose.yml up -d
```

To check the running containers:

```powershell
docker compose -f docker\docker-compose.yml ps
```

To stop the services:

```powershell
docker compose -f docker\docker-compose.yml down
```

## Services and Access Instructions

### MinIO

MinIO is used as the object-storage layer of the RetailLake platform.

The following buckets are used:

| Bucket | Purpose |
|---|---|
| `retaillake-raw` | Stores original source files and API data |
| `retaillake-bronze` | Stores structured Bronze-layer Parquet files |
| `retaillake-silver` | Stores cleaned Silver-layer data and quarantine outputs |

Open the MinIO Console:

http://localhost:9001

The MinIO service must be running through Docker Compose before accessing the console.

### Apache Airflow

Apache Airflow is used to orchestrate the RetailLake data pipeline.

The main DAG is:

```text
retaillake_pipeline
```

The DAG coordinates the following activities:

- Mock data generation
- External API ingestion
- Raw data ingestion
- Bronze-layer processing
- Silver-layer processing
- Gold-layer loading

Open Airflow:

http://localhost:8080

To run the pipeline:

1. Start the Docker services.
2. Open Airflow.
3. Locate the `retaillake_pipeline` DAG.
4. Trigger the DAG manually.
5. Monitor the task statuses and logs.
6. Confirm that the tasks have completed successfully.

### PostgreSQL

PostgreSQL stores the Gold-layer fact and dimension tables.

The main connection details are:

| Setting | Value |
|---|---|
| Host | `localhost` |
| Port | `5432` |
| Database | `retaillake` |
| Schema | `gold` |

Database credentials should be configured through environment variables.

Passwords and secret credentials must not be committed to GitHub.

### Apache Superset

Apache Superset is used to visualise the Gold-layer data.

Dashboard name:

```text
RetailLake Smart Retail Dashboard
```

Open Superset:

http://localhost:8088

The dashboard provides sales, inventory, data-quality, and API-enrichment visualisations.

The Superset dashboard is available when the required Superset and PostgreSQL services are running.

## Running the Pipeline

### Step 1: Generate Mock Data

Run the mock-data generation script:

```powershell
python scripts\seed_mock_sources.py
```

### Step 2: Validate Mock Data

Validate the generated source data:

```powershell
python scripts\validate_mock_sources.py
```

### Step 3: Ingest Raw Data

Run the Raw ingestion process:

```powershell
python scripts\ingest_raw.py
```

This uploads source files into the MinIO Raw bucket.

### Step 4: Ingest External API Data

Run the Open Food Facts API ingestion script:

```powershell
python scripts\ingest_open_food_facts.py
```

Run the DummyJSON API ingestion script:

```powershell
python scripts\ingest_dummyjson.py
```

### Step 5: Process the Bronze Layer

Run the Bronze processing script:

```powershell
python scripts\bronze_all.py
```

### Step 6: Process the Silver Layer

The Silver-layer scripts clean and validate the datasets.

The scripts also separate invalid records into quarantine outputs.

Examples include:

```powershell
python scripts\silver_pos.py
python scripts\silver_ecommerce.py
python scripts\silver_customers.py
python scripts\silver_products.py
python scripts\silver_stores.py
python scripts\silver_inventory.py
python scripts\silver_suppliers.py
python scripts\silver_open_food_facts.py
python scripts\silver_dummyjson.py
```

### Step 7: Load the Gold Layer

The Gold-layer scripts load reporting-ready tables into PostgreSQL.

Examples include:

```powershell
python scripts\gold_dim_customer.py
python scripts\gold_dim_product.py
python scripts\gold_dim_store.py
python scripts\gold_dim_date.py
python scripts\gold_fact_sales.py
python scripts\gold_fact_inventory.py
python scripts\gold_open_food_facts.py
python scripts\gold_dummyjson.py
```

### Step 8: Create the Quarantine Summary

Generate the combined quarantine summary:

```powershell
python scripts\create_quarantine_summary.py
```

Load the quarantine summary into PostgreSQL:

```powershell
python scripts\gold_quarantine_summary.py
```

### Recommended Execution Method

The complete workflow can also be orchestrated using the Airflow DAG:

```text
retaillake_pipeline
```

Airflow is the recommended method for demonstrating the complete automated pipeline.

## Data Quality

The platform performs data-quality checks, including:

- Missing primary keys
- Duplicate records
- Invalid prices
- Invalid quantities
- Missing foreign keys
- Unknown product identifiers
- Invalid dates
- Missing customer identifiers
- Invalid email addresses
- Incorrect data types

Invalid records are stored in quarantine outputs together with the reasons for rejection.

The project also includes automated tests for:

- Project structure
- Dataset sizes
- Invalid source data
- Silver-layer outputs
- Quarantine records

## Testing

The project uses Pytest for automated validation.

Run the tests from the project root:

```powershell
pytest -q
```

The test suite checks the project structure, data quality rules, source data, and Silver-layer outputs.

The latest completed test run passed all 13 tests.

GitHub Actions can be used to run automated tests when changes are pushed to the repository, provided the workflow is configured and enabled.

## Evidence and Demonstration

The following evidence should be captured for the final project demonstration and report:

- MinIO buckets and date-based folders
- Raw-layer files
- Bronze-layer Parquet files
- Silver clean-data outputs
- Quarantine records and error reasons
- Airflow DAG and successful task runs
- PostgreSQL Gold tables
- Superset dashboard and charts
- Automated test results

## Known Limitations

- The platform currently runs locally using Docker Compose.
- The retail datasets are simulated for project purposes.
- External API availability may affect API ingestion.
- The dashboard depends on the availability of local services.
- The platform is not currently deployed to a cloud environment.
- Additional production monitoring and alerting can be added in the future.
- API responses and source data may change over time.

## Repository

GitHub repository:

https://github.com/ShahaniSamsudeen/smart-retail-data-platform

## Author

G. Shahani Samsudeen