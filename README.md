# 🛒 E-Commerce Data Pipeline

End-to-end Data Engineering project: daily fake data generation → 4-layer ETL pipeline → business metrics.

## Architecture

```
Faker (Data Generator)
        │
        ▼
┌─────────────────┐
│   Raw Layer     │  raw_orders, raw_customers, raw_products
└────────┬────────┘
         │  Airflow DAG (daily @ 1:00 AM)
         ▼
┌─────────────────┐
│ Staging Layer   │  Validate + clean + flag invalid rows
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Warehouse Layer │  Star Schema: dim_date, dim_customers (SCD2),
│                 │              dim_products, fct_orders
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│  Marts Layer    │  mart_daily_revenue
│                 │  mart_product_performance
│                 │  mart_customer_segments
└─────────────────┘
```

## Tech Stack

| Tool | Purpose |
|---|---|
| **Python** | Data generation, ETL scripts |
| **Faker** | Realistic fake data (vi_VN locale) |
| **PostgreSQL 15** | Database for all layers |
| **Apache Airflow 2.8** | Pipeline orchestration |
| **Docker + Docker Compose** | Containerization |
| **Git** | Version control |

## Project Structure

```
ecom-pipeline/
├── dags/
│   └── etl_dag.py           # Airflow DAG definition
├── scripts/
│   ├── db.py                # DB connection helper
│   ├── generator.py         # Faker data generator
│   ├── staging.py           # Validate + clean
│   ├── transform.py         # Load to star schema
│   └── marts.py             # Build business metrics
├── sql/
│   ├── 01_source.sql        # Raw tables
│   ├── 02_staging.sql       # Staging tables
│   ├── 03_warehouse.sql     # Dim + fact tables (star schema)
│   └── 04_marts.sql         # Mart tables
├── docker-compose.yml
├── Dockerfile
├── requirements.txt
└── README.md
```

## Quick Start

### 1. Clone & start

```bash
git clone https://github.com/yourusername/ecom-pipeline.git
cd ecom-pipeline
docker-compose up --build -d
```

Wait ~60 seconds for Airflow to initialize.

### 2. Access Airflow UI

```
URL:      http://localhost:8080
Username: admin
Password: admin
```

### 3. Trigger the DAG

- Go to **DAGs** → find `ecom_etl_pipeline`
- Toggle it **ON**
- Click **Trigger DAG** (▶) to run immediately

### 4. Verify data in PostgreSQL

```bash
# Connect to Postgres
docker exec -it ecom_postgres psql -U airflow -d ecom_db

-- Check each layer
SELECT COUNT(*) FROM raw_orders;
SELECT COUNT(*), COUNT(*) FILTER (WHERE is_valid) FROM stg_orders;
SELECT COUNT(*) FROM fct_orders;

-- Check marts
SELECT * FROM mart_daily_revenue ORDER BY report_date DESC LIMIT 5;
SELECT * FROM mart_product_performance ORDER BY revenue DESC LIMIT 10;
SELECT * FROM mart_customer_segments ORDER BY total_revenue DESC LIMIT 10;
```

## DAG Flow

```
generate_daily_data
        │  ~250 fake orders/day using Faker
        ▼
    run_staging
        │  Validate: quantity > 0, price > 0, email format, foreign keys
        │  Flag invalid rows with reason (never delete raw data)
        ▼
  load_warehouse
        │  dim_customers: SCD Type 2 (track customer changes)
        │  dim_products: upsert
        │  fct_orders: only valid + linked orders
        ▼
   build_marts
        │  Daily revenue, cancel rate, avg order value
        │  Product performance by category
        └  Customer segments by city + age group
```

## Key Concepts Demonstrated

- **4-layer data architecture**: Raw → Staging → Warehouse → Marts
- **Star schema design**: fact + dimension tables
- **SCD Type 2**: tracking customer changes over time
- **Data validation**: is_valid flag + invalid_reason (audit trail)
- **Idempotency**: `ON CONFLICT DO NOTHING/UPDATE` everywhere
- **Orchestration**: Airflow DAG with task dependencies
- **Containerization**: Full stack in Docker Compose

## Stopping the project

```bash
docker-compose down          # stop containers
docker-compose down -v       # stop + remove volumes (reset all data)
```
