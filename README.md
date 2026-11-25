# 🚀 Enterprise ETL Data Pipeline & Analytics Warehouse

[![Python](https://img.shields.io/badge/Python-3.10%2B-blue.svg)](https://www.python.org/)
[![Pandas](https://img.shields.io/badge/Pandas-2.0%2B-150458.svg)](https://pandas.pydata.org/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16-336791.svg)](https://www.postgresql.org/)
[![AWS S3](https://img.shields.io/badge/AWS%20S3-Cloud%20Storage-569A31.svg)](https://aws.amazon.com/s3/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED.svg)](https://www.docker.com/)
[![License](https://img.shields.io/badge/License-MIT-green.svg)](LICENSE)

An end-to-end, production-grade ETL (Extract, Transform, Load) data pipeline and analytics warehouse. This system extracts multi-table transactional e-commerce data from CSV files and AWS S3, cleans and standardizes it using **Python (Pandas)**, enforces rigorous data quality assertions, and loads the data into a **Star-Schema Data Warehouse** (**PostgreSQL** with zero-configuration **SQLite** fallback).

The warehouse processes **11,800+ transaction line items** across customers, products, and retail channels, and powers advanced analytical SQL queries (CTEs, Window Functions, Rollups) to unlock business insights.

---

## 📑 Table of Contents
- [Architecture Overview](#-architecture-overview)
- [Star-Schema Data Model](#-star-schema-data-model)
- [Key Features](#-key-features)
- [Tech Stack](#-tech-stack)
- [Project Directory Structure](#-project-directory-structure)
- [Quick Start Guide](#-quick-start-guide)
  - [Option A: Instant Local Run (SQLite)](#option-a-instant-local-run-sqlite)
  - [Option B: Production Mode (PostgreSQL + Docker)](#option-b-production-mode-postgresql--docker)
  - [Option C: Interactive Web Dashboard & Hosting](#option-c-interactive-web-dashboard--hosting)
- [Data Pipeline Stages](#-data-pipeline-stages)
  - [1. Extraction (S3 & CSV)](#1-extraction-s3--csv)
  - [2. Transformation & Modeling (Pandas)](#2-transformation--modeling-pandas)
  - [3. Validation & Quarantine](#3-validation--quarantine)
  - [4. Loading & Lineage Tracking](#4-loading--lineage-tracking)
- [Analytical Business Queries](#-analytical-business-queries)
- [Testing & Quality Assurance](#-testing--quality-assurance)

---

## 🏗 Architecture Overview

```
 ┌────────────────┐     ┌────────────────┐
 │   Local CSVs   │     │  AWS S3 Bucket │
 │  (10K+ Rows)   │     │ (Raw / Landing)│
 └───────┬────────┘     └────────┬───────┘
         │                       │
         └───────────┬───────────┘
                     │
                     ▼
         ┌───────────────────────┐
         │     Extract Stage     │  Schema detection, file checksums,
         │   (src/extract.py)    │  batch identification
         └───────────┬───────────┘
                     │
                     ▼
         ┌───────────────────────┐  Data cleaning, deduplication,
         │    Transform Stage    │  multi-format datetime parsing,
         │  (src/transform.py)   │  financial calculations, surrogate keys
         └───────────┬───────────┘
                     │
                     ▼
         ┌───────────────────────┐  Null checks, range boundaries,
         │    Validate Stage     │  referential integrity assertions
         │   (src/validate.py)   │
         └─────┬───────────┬─────┘
               │           │
       Passed  │           │ Quarantined
               ▼           ▼
  ┌──────────────────┐   ┌──────────────────────┐
  │ Star-Schema Load │   │ data/rejected/       │
  │ (src/load.py)    │   │ etl_rejected_records │
  └────────┬─────────┘   └──────────────────────┘
           │
           ├──────────────────────────────┐
           ▼                              ▼
  ┌─────────────────────┐      ┌─────────────────────┐
  │  PostgreSQL (Prod)  │      │   SQLite (Local)    │
  │ Analytics Warehouse │      │ Analytics Warehouse │
  └────────┬────────────┘      └──────────┬──────────┘
           │                              │
           └──────────────┬───────────────┘
                          │
                          ▼
              ┌───────────────────────┐
              │  SQL Business Metrics │  Monthly growth, customer LTV,
              │ (sql/analytics_*.sql) │  RFM segmentation, category margin
              └───────────────────────┘
```

---

## 🌟 Star-Schema Data Model

The data warehouse implements a dimensional **Star-Schema** optimized for analytical read performance, OLAP queries, and reporting.

```
                      ┌──────────────────────┐
                      │       dim_date       │
                      ├──────────────────────┤
                      │ PK  date_key         │
                      │     full_date        │
                      │     year, quarter    │
                      │     month, month_name│
                      │     is_weekend       │
                      └──────────┬───────────┘
                                 │
                                 │ 1:N
┌──────────────────────┐         │         ┌──────────────────────┐
│     dim_customer     │         │         │     dim_product      │
├──────────────────────┤         ▼         ├──────────────────────┤
│ PK  customer_key     ├──┐   ┌─────────┐  │ PK  product_key      │
│     customer_id (NK) │  └──►│         │◄─┤     product_id (NK)  │
│     first_name       │      │  fact_  │  │     product_name     │
│     last_name        │      │  sales  │  │     category         │
│     email            │  ┌──►│         │◄─┤     subcategory      │
│     city, state      │  │   └─────────┘  │     cost_price       │
│     customer_segment │  │        ▲       │     list_price       │
└──────────────────────┘  │        │       │     margin_pct       │
                          │        │ 1:N   └──────────────────────┘
┌──────────────────────┐  │        │
│      dim_store       │  │        │       ┌──────────────────────┐
├──────────────────────┤  │        │       │  dim_payment_method  │
│ PK  store_key        ├──┘        │       ├──────────────────────┤
│     store_id (NK)    │           └───────┤ PK  payment_key      │
│     store_name       │                   │     payment_method   │
│     store_type       │                   │     payment_provider │
│     region, country  │                   │     is_digital       │
└──────────────────────┘                   └──────────────────────┘
```

### Fact Table: `fact_sales`
| Column | Type | Description |
| :--- | :--- | :--- |
| `sale_id` | INT / SERIAL (PK) | Auto-incrementing surrogate key |
| `order_id` | VARCHAR(50) | Degenerate dimension (order business key) |
| `order_item_id`| VARCHAR(50) (Unique) | Unique line item identifier |
| `date_key` | INT (FK) | Reference to `dim_date.date_key` |
| `customer_key`| INT (FK) | Reference to `dim_customer.customer_key` |
| `product_key` | INT (FK) | Reference to `dim_product.product_key` |
| `store_key` | INT (FK) | Reference to `dim_store.store_key` |
| `payment_key` | INT (FK) | Reference to `dim_payment_method.payment_key` |
| `quantity` | INT | Quantity purchased (validated > 0) |
| `unit_price` | NUMERIC(12, 2) | Price per unit at purchase time |
| `discount_pct`| NUMERIC(5, 2) | Applied discount percentage (0 - 100%) |
| `discount_amount`| NUMERIC(12, 2) | Gross Amount * Discount % |
| `gross_amount`| NUMERIC(12, 2) | Quantity * Unit Price |
| `tax_amount` | NUMERIC(12, 2) | 8% Estimated sales tax |
| `net_amount` | NUMERIC(12, 2) | Gross - Discount + Tax (Total customer invoice) |
| `cost_amount` | NUMERIC(12, 2) | Quantity * Cost Price (COGS) |
| `profit_amount`| NUMERIC(12, 2) | Net Sales (excl tax) - COGS |
| `etl_batch_id`| VARCHAR(100) | Audit lineage tracking identifier |

---

## ⚡ Key Features

- **Multi-Source Ingestion**: Ingests raw data from local filesystem or directly from **AWS S3** buckets via `boto3`. Features an automatic local mock mode for offline execution.
- **Robust Transformation Engine (Pandas)**:
  - Cleans whitespace, standardizes email casing, formats names with title casing.
  - Multi-pattern datetime parser handling `%Y-%m-%d %H:%M:%S`, `%d/%m/%Y %H:%M:%S`, and ISO strings.
  - Automatic deduplication across primary and natural keys.
  - Calculation of derived financial metrics (`gross_amount`, `discount_amount`, `tax_amount`, `net_amount`, `cost_amount`, `profit_amount`).
- **Data Validation & Quarantine Engine**:
  - Null checks on required business columns.
  - Boundary assertions: `quantity > 0`, `unit_price >= 0`, `0 <= discount_pct <= 100`.
  - Referential integrity checks between fact and dimensions before warehouse insertion.
  - Bad records are quarantined to `data/rejected/` with timestamp and failure diagnostics.
- **Enterprise Star Schema DDL**: Full PostgreSQL DDL (`sql/schema.sql`) and SQLite DDL (`sql/schema_sqlite.sql`) with B-Tree analytical indexing (`sql/indexes.sql`).
- **Complete Pipeline Lineage & Audit**: `etl_audit_log` records batch execution time, status, extracted row count, loaded row count, and error messages.
- **Actionable Business Analytics**: 7 analytical SQL scripts analyzing Monthly Revenue, Customer Lifetime Value, Category Profitability, Store Regional performance, and Payment Channel AOV across 10K+ records.

---

## 🛠 Tech Stack

| Component | Technology | Description |
| :--- | :--- | :--- |
| **Language** | Python 3.10+ | Core ETL execution engine |
| **Data Processing** | Pandas, NumPy | Cleaning, vector calculations, dimensional mapping |
| **Data Warehouse** | PostgreSQL 16 | Production target warehouse |
| **Local Storage** | SQLite 3 | Zero-configuration testing & local evaluation |
| **Cloud Storage** | AWS S3 (boto3) | Raw file storage & mock landing bucket |
| **Containerization**| Docker & Docker Compose | Containerized PostgreSQL and pgAdmin setup |
| **Configuration** | PyYAML, python-dotenv | Environment and pipeline parameters |
| **Logging** | RotatingFileHandler | Thread-safe formatted console and file logging |

---

## 📂 Project Directory Structure

```
etl/
├── config/
│   ├── config.yaml              # Pipeline, database, and S3 configuration
│   └── logging_config.py        # Rotating log file and console formatter
├── data/
│   ├── raw/                     # Extracted raw CSV datasets (>10,000 records)
│   ├── processed/               # Transformed clean CSV datasets
│   ├── rejected/                # Quarantined records & JSON validation reports
│   └── analytics_warehouse.db   # Local SQLite data warehouse database
├── logs/
│   └── etl.log                  # Comprehensive pipeline execution logs
├── sql/
│   ├── schema.sql               # PostgreSQL Star-Schema DDL
│   ├── schema_sqlite.sql        # SQLite Star-Schema DDL
│   ├── indexes.sql              # Analytical B-Tree index definitions
│   └── analytics_queries.sql    # 7 production SQL analytical queries
├── src/
│   ├── __init__.py
│   ├── generator.py             # Realistic 10,000+ row synthetic data generator
│   ├── s3_utils.py              # AWS S3 synchronization with local mock fallback
│   ├── extract.py               # CSV and S3 extraction engine
│   ├── transform.py             # Pandas cleaning and star-schema modeler
│   ├── validate.py              # Data quality checker & quarantine handler
│   ├── load.py                  # Transactional database loader & audit logger
│   └── analytics_runner.py      # SQL query runner & formatted KPI reporter
├── tests/
│   ├── test_extract.py          # Unit tests for extraction
│   ├── test_transform.py        # Unit tests for Pandas transformations
│   ├── test_validate.py         # Unit tests for data validation & quarantine
│   ├── test_pipeline_e2e.py     # End-to-end integration tests
│   └── run_tests.py             # Test runner using built-in unittest
├── docker-compose.yml           # PostgreSQL 16 + pgAdmin 4 local environment
├── requirements.txt             # Python project dependencies
├── .env.example                 # Environment variables template
├── run_pipeline.py              # Main CLI pipeline orchestrator
└── README.md                    # Project documentation
```

---

## 🚀 Quick Start Guide

### Prerequisites
- Python 3.10 or higher
- Git

### Installation
```bash
# Clone the repository
git clone https://github.com/your-username/etl-data-pipeline.git
cd etl-data-pipeline

# Install dependencies
pip install -r requirements.txt
```

---

### Option A: Instant Local Run (SQLite)
Run the complete pipeline end-to-end with **zero setup** (runs on the bundled SQLite warehouse engine):

```bash
# Run complete pipeline: Generate -> Extract -> Transform -> Validate -> Load -> Analytics
python run_pipeline.py
```

#### Selective Stage Execution:
```bash
# 1. Generate 12,000+ synthetic transactional records
python run_pipeline.py --generate

# 2. Extract raw data from CSV / S3
python run_pipeline.py --extract

# 3. Clean and transform data into star-schema tables
python run_pipeline.py --transform

# 4. Run data validation rules and quarantine invalid rows
python run_pipeline.py --validate

# 5. Load clean tables into warehouse
python run_pipeline.py --load

# 6. Run SQL analytics and display KPIs
python run_pipeline.py --analytics
```

---

### Option B: Production Mode (PostgreSQL + Docker)

1. **Spin up PostgreSQL 16 and pgAdmin 4**:
   ```bash
   docker compose up -d
   ```
   - PostgreSQL will be running on `localhost:5432`
   - pgAdmin will be available at `http://localhost:5050` (Login: `admin@warehouse.com` / `admin`)

2. **Configure environment variables**:
   ```bash
   cp .env.example .env
   ```
   Ensure `.env` contains:
   ```env
   DB_TYPE=postgresql
   DB_HOST=localhost
   DB_PORT=5432
   DB_NAME=analytics_warehouse
   DB_USER=warehouse_admin
   DB_PASSWORD=admin_password
   ```

3. **Run the pipeline targeted at PostgreSQL**:
   ```bash
   python run_pipeline.py --full-run --db postgresql
   ```

---

### Option C: Interactive Web Dashboard & Hosting

Launch the web analytics dashboard and SQL query console:

```bash
# Start the web server
python app.py
```
Open your browser and navigate to **`http://localhost:8080`**.

#### Features Available in the Web Dashboard:
- **Live KPI Counters**: Instant view of Total Revenue, Net Profit, Margin %, and Quarantined rows.
- **Interactive Visualizations**: Monthly growth bar chart, product category profit comparison, regional revenue doughnut chart, and payment method AOV.
- **Interactive SQL Runner**: Write and run ad-hoc `SELECT` queries directly against the warehouse tables with instant table formatting.
- **Pipeline Trigger**: Execute new ETL batches directly from the UI with real-time status updates.
- **Audit Log Inspector**: View batch execution lineage, status, and duration records from `etl_audit_log`.

#### Cloud Hosting (Docker / Render / Railway):
The repository contains ready-to-deploy configurations:
- **Docker**: `docker build -t etl-warehouse . && docker run -p 8080:8080 etl-warehouse`
- **Render**: Connect the repository and select **Web Service** using the bundled `render.yaml` or `Dockerfile`.
- **Railway / Heroku**: Deploys automatically via the included `Procfile` and `requirements.txt`.

---

## 📊 Analytical Business Queries

The pipeline includes 7 SQL queries (`sql/analytics_queries.sql`) that execute joins across fact and dimension tables.

### 1. Monthly Revenue & Month-over-Month (MoM) Growth
Demonstrates: **CTEs**, **Date Joins**, and **Window Functions (`LAG`)**.
```sql
WITH monthly_metrics AS (
    SELECT 
        d.year,
        d.month,
        d.month_name,
        COUNT(DISTINCT f.order_id) AS total_orders,
        SUM(f.quantity) AS total_units_sold,
        ROUND(SUM(f.net_amount), 2) AS total_revenue,
        ROUND(SUM(f.profit_amount), 2) AS total_profit,
        ROUND((SUM(f.profit_amount) / NULLIF(SUM(f.net_amount), 0)) * 100, 2) AS profit_margin_pct
    FROM fact_sales f
    JOIN dim_date d ON f.date_key = d.date_key
    GROUP BY d.year, d.month, d.month_name
)
SELECT 
    year,
    month,
    month_name,
    total_orders,
    total_revenue,
    total_profit,
    profit_margin_pct,
    LAG(total_revenue, 1) OVER (ORDER BY year, month) AS prev_month_revenue,
    ROUND(
        ((total_revenue - LAG(total_revenue, 1) OVER (ORDER BY year, month)) / 
         NULLIF(LAG(total_revenue, 1) OVER (ORDER BY year, month), 0)) * 100, 
        2
    ) AS mom_growth_pct
FROM monthly_metrics
ORDER BY year, month;
```

### 2. Category Profitability & Effective Discount Sensitivity
```sql
SELECT 
    p.category,
    p.subcategory,
    COUNT(DISTINCT f.order_id) AS total_transactions,
    SUM(f.quantity) AS total_units_sold,
    ROUND(SUM(f.gross_amount), 2) AS gross_sales,
    ROUND(SUM(f.discount_amount), 2) AS total_discounts,
    ROUND(SUM(f.net_amount), 2) AS net_revenue,
    ROUND(SUM(f.profit_amount), 2) AS net_profit,
    ROUND((SUM(f.profit_amount) / NULLIF(SUM(f.net_amount), 0)) * 100, 2) AS profit_margin_pct
FROM fact_sales f
JOIN dim_product p ON f.product_key = p.product_key
GROUP BY p.category, p.subcategory
ORDER BY net_profit DESC;
```

### 3. Executive KPI Summary Output
```
======================================================================
METRIC: WAREHOUSE KPI SUMMARY
======================================================================
Total Sales Records  : 11,760
Unique Orders        : 5,980
Active Customers     : 2,500
Total Products       : 120
Retail Stores        : 13
Total Revenue        : $5,008,889.23
Total Net Profit     : $1,833,262.64
Warehouse Net Margin : 36.6%
======================================================================
```

---

## 🛡 Data Quality, Validation & Lineage

### Automated Checks
| Check Category | Validation Rule | Action on Failure |
| :--- | :--- | :--- |
| **Null Value Assertions** | `customer_key`, `product_key`, `date_key`, `net_amount` cannot be null | Halts batch or drops row |
| **Range Checks** | `quantity > 0` | Quarantines record to `data/rejected/` |
| **Price Checks** | `unit_price >= 0` | Quarantines record to `data/rejected/` |
| **Discount Threshold** | `0.0 <= discount_pct <= 100.0` | Quarantines record to `data/rejected/` |
| **Uniqueness** | Natural Keys and Surrogate Keys must be unique | Automatically deduplicated |
| **Referential Integrity**| All FKs in `fact_sales` must exist in dimension tables | Logs orphan count and halts |

### Audit Log Sample (`etl_audit_log`)
Every execution automatically logs metadata for end-to-end lineage:
```sql
SELECT batch_id, rows_extracted, rows_loaded, rows_rejected, status, execution_duration_sec 
FROM etl_audit_log 
ORDER BY start_time DESC LIMIT 1;
```
| batch_id | rows_extracted | rows_loaded | rows_rejected | status | execution_duration_sec |
| :--- | :--- | :--- | :--- | :--- | :--- |
| `BATCH-20261125154226` | 20,475 | 11,760 | 69 | `SUCCESS` | 0.42 |

---

## 🧪 Testing & Quality Assurance

Run the test suite using Python's built-in test runner:
```bash
python tests/run_tests.py
```

### Test Coverage:
- **Extraction**: Validates expected raw files, format parsing, and non-empty file assertions.
- **Transformation**: Validates deduplication, title-casing normalization, margin formulas, and flexible multi-format datetime parsing.
- **Validation**: Asserts that negative quantities, negative prices, and invalid discounts are caught and quarantined.
- **Warehouse Integration**: Verifies table schemas, foreign key constraints, and record volume (>10K rows) in the warehouse database.

---

## 📜 License
This project is open-source and available under the [MIT License](LICENSE).
