# Payment Risk and Order Analytics Platform

End-to-end data platform for payment fraud detection and order fulfillment risk assessment, built on Brazilian e-commerce marketplace data (Olist).

## Overview

This platform ingests 100K+ e-commerce orders, transforms them through a Kimball-modeled data warehouse, and serves real-time risk scores via REST API. The pipeline orchestrates bronze → staging → mart layers using dbt, Airflow, and PostgreSQL.

**Tech Stack:** Python 3.11 | PostgreSQL 15 | dbt 1.7 | Apache Airflow 2.8 | FastAPI | WSL2

## Architecture

```
Data Flow:
  Olist CSV Files (8 files, 1M+ rows)
       |
       v
  [Bronze Layer] - Raw ingestion (faithful copy)
       |
       v
  [Staging Layer] - Cleansing & type casting (dbt views)
       |
       v
  [Intermediate Layer] - Feature engineering (dbt ephemeral)
       |
       v
  [Mart Layer] - Kimball star schema
       |
       +-- 5 Dimensions (SCD Type 2 on customer/seller)
       +-- 2 Facts (payment risk, fulfillment risk)
       +-- 1 KPI aggregate (daily risk metrics)
       |
       v
  [FastAPI] - Risk scoring endpoints
```

## Key Features

### Data Pipeline
- **Bronze Layer**: Idempotent CSV ingestion with audit columns
- **Staging Layer**: NaN-to-NULL conversion, data type casting
- **Mart Layer**: Two-fact design to prevent measure corruption
- **SCD Type 2**: Historical tracking for customer/seller dimensions
- **Airflow Orchestration**: Quality gates, branching logic, scheduled snapshots

### Risk Scoring
- **Payment Risk Model**: High-value orders, installment patterns, dispute proxies
- **Fulfillment Risk Model**: Late delivery prediction, seller reliability
- **REST API**: 6 endpoints serving risk scores and platform metrics

### Data Quality
- 95+ automated tests (pytest)
- Bronze row count validation
- NULL handling verification
- SCD2 join correctness checks
- Regression suite for NaN bug fix

## Project Structure

```
payment-risk-platform/
├── airflow/dags/           # Orchestration DAGs
│   ├── payment_ingest.py        # Bronze → staging (every 15 min)
│   └── payment_mart_refresh.py  # Marts + snapshots (every 30 min)
├── apps/risk_api/          # FastAPI service
│   └── main.py                  # 6 REST endpoints
├── dbt/                    # Data transformations
│   ├── models/
│   │   ├── staging/             # 8 views
│   │   ├── intermediate/        # 4 ephemeral models
│   │   └── marts/
│   │       ├── dimensions/      # 5 dimension tables
│   │       └── facts/           # 2 fact tables + 1 KPI
│   └── snapshots/               # SCD2 tracking
├── ingestion/
│   ├── loader/load_bronze.py    # CSV → PostgreSQL
│   └── setup_warehouse.py       # DDL execution
├── sql/ddl/                # Schema definitions
├── tests/                  # 95+ tests (94% pass rate)
│   ├── unit/                    # Config, loader, DAG validation
│   ├── integration/             # Pipeline, data quality, snapshots
│   ├── regression/              # NaN fix, SCD2 joins
│   └── performance/             # dbt execution time
├── config.py               # Centralized configuration
├── Makefile                # Common commands
└── requirements.txt
```

## Setup Instructions

### Prerequisites
- WSL2 Ubuntu 24.04 (or native Linux)
- Python 3.11
- PostgreSQL 15 (accessible from WSL2)

### Installation

1. **Clone repository**
```bash
git clone https://github.com/shariq02/payment-risk-platform.git
cd payment-risk-platform
```

2. **Install dependencies**
```bash
make setup
# OR
pip install -r requirements.txt
```

3. **Configure environment**
```bash
cp .env.example .env
# Edit .env with your PostgreSQL credentials
```

4. **Initialize database**
```bash
python ingestion/setup_warehouse.py
```

5. **Load bronze data**
```bash
python ingestion/loader/load_bronze.py
```

6. **Run dbt models**
```bash
make dbt-run
make dbt-test
make dbt-snapshot
```

7. **Initialize Airflow** (optional)
```bash
export AIRFLOW_HOME=~/airflow
airflow db init
airflow users create --username admin --password admin123 --firstname Admin --lastname User --role Admin --email admin@example.com
```

## Usage

### Run Full Pipeline
```bash
make dbt-run          # Build all marts
make dbt-snapshot     # Create SCD2 snapshots
```

### Start FastAPI Server
```bash
make api
# Server runs on http://localhost:8000
# API docs: http://localhost:8000/docs
```

### API Endpoints

**Health Check**
```bash
curl http://localhost:8000/health
```

**Customer Risk Profile**
```bash
curl http://localhost:8000/risk/customer/{customer_id}
```

**Seller Risk Summary**
```bash
curl http://localhost:8000/risk/seller/{seller_id}/summary
```

**Top Payment Alerts**
```bash
curl "http://localhost:8000/risk/top-payment-alerts?limit=20&min_risk_score=0.7"
```

**Platform Statistics**
```bash
curl http://localhost:8000/platform/stats
```

### Airflow DAGs

Start Airflow scheduler and webserver:
```bash
airflow scheduler &
airflow webserver -p 8080 &
```

Access UI: http://localhost:8080

**DAGs:**
- `payment_ingest`: Loads bronze, runs quality gates (every 15 min)
- `payment_mart_refresh`: Refreshes marts, runs snapshots at 2 AM (every 30 min)

### Run Tests
```bash
make test                    # All tests
pytest tests/unit/ -v        # Unit tests only
pytest tests/integration/ -v # Integration tests
```

### Generate dbt Documentation
```bash
make dbt-docs
# Opens browser with interactive lineage graph
```

## Data Model

### Fact Tables

**fact_order_payments** (103,886 rows)
- Grain: One row per payment
- Measures: payment_value, payment_risk_score
- Dimensions: customer_sk, geo_sk, payment_method_sk, time_sk

**fact_order_fulfillment** (112,650 rows)  
- Grain: One row per order item
- Measures: item_price, freight_value, fulfillment_risk_score
- Dimensions: seller_sk, product_sk, time_sk

**Why Two Facts?**
Using separate fact tables prevents measure corruption. Payment-level metrics (total order value, installments) have different granularity than item-level metrics (freight cost, delivery time). Joining them in a single fact would require complex aggregation logic and risk double-counting.

### Dimension Tables

**dim_customer** (96,095 rows) - SCD Type 2
- Tracks: total_orders, total_payment_value, risk_tier, segment
- Changes tracked via: valid_from, valid_to, is_current

**dim_seller** (3,095 rows) - SCD Type 2  
- Tracks: seller_reliability_score, late_delivery_rate, avg_review_score
- Changes tracked via: valid_from, valid_to, is_current

**dim_product** (32,951 rows) - Type 1
**dim_geo** (19,015 rows) - Type 1
**dim_time** (26,304 rows) - Date dimension

### Point-in-Time Join Example
```sql
-- Correct way to join SCD2 dimensions with facts
SELECT 
    f.order_id,
    d.total_orders,
    d.risk_tier_code
FROM mart.fact_order_payments f
INNER JOIN mart.dim_customer d
    ON f.customer_unique_id = d.customer_unique_id
    AND f.event_ts >= d.valid_from
    AND (d.valid_to IS NULL OR f.event_ts < d.valid_to);
```

## Performance

- **dbt full pipeline**: 19 seconds (16 models)
- **dbt tests**: 81 tests passing
- **Bronze load**: ~5 seconds (1M+ rows)
- **API response time**: <100ms (p95)

## Testing Strategy

**95 tests across 4 categories:**
- Unit (24): Config, loader functions, DAG structure
- Integration (47): Full pipeline, data quality, snapshots, API queries
- Regression (20): NaN fix, SCD2 joins, historical accuracy
- Performance (4): dbt execution time thresholds

**Key Test Coverage:**
- ✓ NaN values stored as NULL, not string 'NaN'
- ✓ All fact foreign keys have matching dimension records
- ✓ SCD2 snapshots create new records on attribute changes
- ✓ Row counts match expected (bronze: 99,441 orders, 112,650 items)
- ✓ Risk scores within 0-1 range

## License

MIT License - See LICENSE file for details

## Acknowledgments

- Olist dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
- Product category translation sourced from Olist public repository
