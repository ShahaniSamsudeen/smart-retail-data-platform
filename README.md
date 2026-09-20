
# RetailLake Smart Retail Data Platform

A local smart retail data platform built using a modern data engineering pipeline.

The platform generates retail data, ingests data from multiple sources and APIs, processes it through Raw, Bronze, Silver and Gold layers, validates data quality, and presents business insights through Apache Superset.

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
- Automate testing using GitHub Actions.

## Architecture

Mock Data Sources and APIs  
↓  
Apache Airflow  
↓  
MinIO - Raw Layer  
↓  
Bronze Layer  
↓  
Silver Layer  
├── Clean Data  
└── Quarantine Data  
↓  
PostgreSQL - Gold Layer  
↓  
Apache Superset Dashboard

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
| Continuous Integration | GitHub Actions |
| Version Control | Git and GitHub |

## Medallion Architecture

### Raw Layer

Stores original source files in MinIO using date-based folder structures.

### Bronze Layer

Parses source files into structured and typed data while preserving ingestion metadata.

### Silver Layer

Cleans and validates data, handles duplicates, checks invalid values, and separates invalid records into quarantine storage.

### Gold Layer

Contains reporting-ready fact and dimension tables in PostgreSQL.

### BI Layer

Apache Superset uses Gold-layer data to display sales, inventory, data-quality and API-enrichment insights.

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

## Dashboard Charts

The Superset dashboard includes:

- Daily sales revenue trend
- Revenue by sales channel
- Top products by net sales
- Low-stock and out-of-stock products
- Data-quality summary
- Open Food Facts product information
- DummyJSON product information

## Project Structure

```text
smart-retail-data-platform/
│
├── .github/
│   └── workflows/
│       └── tests.yml
│
├── dags/
├── data/
│   ├── raw/
│   └── processed/
│
├── docker/
├── scripts/
├── src/
├── tests/
├── .env.example
├── README.md
└── requirements.txt
```

## Setup Instructions

### 1. Clone the repository

```bash
git clone https://github.com/ShahaniSamsudeen/smart-retail-data-platform.git
cd smart-retail-data-platform
```

### 2. Create a virtual environment

```bash
python -m venv .venv
```

### 3. Activate the virtual environment on Windows

```powershell
.venv\Scripts\Activate.ps1
```

### 4. Install dependencies

```powershell
pip install -r requirements.txt
pip install pytest
```

### 5. Start Docker services

```powershell
docker compose -f docker\docker-compose.yml up -d
```

### 6. Generate mock data

```powershell
python scripts\seed_mock_sources.py
```

### 7. Validate mock data

```powershell
python scripts\validate_mock_sources.py
```

### 8. Run the tests

```powershell
pytest
```

## Service URLs

| Service | URL |
|---|---|
| MinIO Console | http://localhost:9001 |
| Airflow | http://localhost:8080 |
| Superset | http://localhost:8088 |
| PostgreSQL | localhost:5432 |

## API Integrations

The project integrates the following APIs:

- **Open Food Facts API:** Product and food-related information.
- **DummyJSON API:** Product information for enrichment and demonstration.

API data is processed through the Raw, Bronze, Silver and Gold layers.

## Data Quality

The platform performs checks including:

- Missing primary keys
- Duplicate records
- Invalid prices
- Invalid quantities
- Missing foreign keys
- Unknown product identifiers
- Invalid dates
- Missing customer identifiers

Invalid records are stored separately in quarantine outputs together with error reasons.

## Testing

The project uses Pytest for automated validation.

GitHub Actions automatically runs the project tests when changes are pushed to the `main` branch.

## Known Limitations

- The platform currently runs locally using Docker Compose.
- The retail datasets are simulated for project purposes.
- External API availability may affect API ingestion.
- The dashboard depends on the availability of local services.
- Additional production monitoring can be added in the future.

## Repository

GitHub repository:

https://github.com/ShahaniSamsudeen/smart-retail-data-platform

## Author

G. Shahani Samsudeen