# Retail Data Lakehouse Pipeline

A data lakehouse built on Azure that processes retail sales data through Bronze/Silver/Gold layers using Databricks and PySpark, builds a star schema, and loads it into Azure SQL for reporting.

---

## How It Works

```
┌─────────────┐     ┌──────────────────┐     ┌───────────────────────────────────────────────┐     ┌──────────────┐
│  CSV Source  │────▶│  Azure Data      │────▶│           Azure Data Lake Gen2                │────▶│  Azure SQL   │
│  (Raw Data)  │     │  Factory (ADF)   │     │  ┌─────────┐ ┌─────────┐ ┌─────────────────┐ │     │  Database    │
└─────────────┘     │  - Pipelines     │     │  │ Bronze  │▶│ Silver  │▶│ Gold            │ │     │  - Star      │
                    │  - Triggers      │     │  │ (Raw)   │ │(Cleaned)│ │(Star Schema)    │ │     │    Schema    │
                    │  - Orchestration │     │  │ Delta   │ │ Parquet │ │ Delta           │ │     │  - Reports   │
                    └──────────────────┘     │  └─────────┘ └─────────┘ └─────────────────┘ │     └──────────────┘
                                             └───────────────────────────────────────────────┘
                                                          ▲
                                                          │
                                                 ┌────────────────┐
                                                 │   Databricks   │
                                                 │   (PySpark)    │
                                                 │   - ETL        │
                                                 │   - Window Fn  │
                                                 │   - SCD Type 2 │
                                                 └────────────────┘
```

---

## Project Structure

```
retail-data-lakehouse/
│
├── config/
│   ├── pipeline_config.py              # Centralized pipeline configuration
│   └── azure_config.template.json      # Azure resource settings template
│
├── scripts/
│   └── generate_sample_data.py         # Realistic sales data generator
│
├── databricks/
│   ├── 01_bronze_ingestion.py          # Bronze layer: raw CSV → Delta
│   ├── 02_silver_transformation.py     # Silver layer: cleaning & enrichment
│   ├── 03_gold_star_schema.py          # Gold layer: star schema build
│   ├── 04_load_to_sql.py              # Gold → Azure SQL load
│   └── utils/
│       ├── logger.py                   # Pipeline audit logger
│       └── helpers.py                  # Shared utility functions
│
├── adf_pipelines/
│   ├── pipeline_ingest_raw.json        # ADF: raw data ingestion
│   ├── pipeline_bronze_to_silver.json  # ADF: trigger Silver notebook
│   ├── pipeline_silver_to_gold.json    # ADF: trigger Gold notebook
│   ├── pipeline_gold_to_sql.json       # ADF: load to SQL + quality checks
│   ├── pipeline_master_orchestrator.json # ADF: end-to-end orchestration
│   └── trigger_daily_ingestion.json    # ADF: scheduled daily trigger
│
├── sql/
│   ├── create_tables.sql               # DDL for star schema tables
│   ├── stored_procedures.sql           # SPs for upserts & quality checks
│   └── analytical_queries.sql          # 10 BI-ready analytical queries
│
├── tests/
│   ├── test_transformations.py         # Unit tests for transformations
│   └── test_data_quality.py            # Data quality validation tests
│
├── requirements.txt
├── .gitignore
└── README.md
```

---

## Medallion Layers

### Bronze (Raw Zone)
- **Purpose**: Land raw data as-is with full lineage metadata
- **Format**: Delta Lake
- **Key Features**:
  - Schema enforcement with permissive mode
  - Row-level hash for change detection
  - Source file tracking (`_source_file`)
  - Ingestion timestamp for watermarking
  - Date-partitioned storage

### Silver (Cleaned Zone)
- **Purpose**: Cleaned, standardized, enriched data
- **Format**: Delta Lake (Parquet-backed)
- **Key Features**:
  - Duplicate removal using window functions
  - Null value handling with business defaults
  - Text standardization (initcap, trim, upper)
  - Type casting and date parsing
  - Calculated columns (total_amount, net_amount)
  - Bad record quarantine with reason codes
  - Window function analytics (lag, lead, dense_rank, running totals)
  - Incremental load with watermark tracking

### Gold (Business Ready)
- **Purpose**: Star schema dimensional model
- **Format**: Delta Lake
- **Tables**:
  | Table | Type | Key Feature |
  |-------|------|-------------|
  | `dim_date` | Date dimension | Calendar + fiscal years, holidays |
  | `dim_customer` | SCD Type 2 | Tracks segment changes over time |
  | `dim_product` | SCD Type 2 | Tracks price changes |
  | `dim_store` | Slowly changing | Store location tracking |
  | `fact_sales` | Fact table | Surrogate key joins, unknown member (-1) |

---

## What's In Here

### ETL & Data Processing
- Parameterized ADF pipelines with trigger-based ingestion
- Databricks PySpark transformations across all layers
- Incremental load logic with watermark tracking
- Late-arriving data handling (configurable window)
- Idempotent reload pattern (delete + insert)

### Data Quality
- Input/output data profiling with null analysis
- Quarantine zone for bad records with reason codes
- Schema validation utilities
- Post-load referential integrity checks
- Configurable quality thresholds

### PySpark Window Functions
- **Window Functions**: `row_number`, `dense_rank`, `lag`, `lead`, `percent_rank`
- **Running aggregations**: cumulative revenue per store
- **Customer purchase sequencing**: tracks order history
- **Category ranking**: revenue rank within product categories

### Data Modeling
- Star schema with surrogate keys
- SCD Type 2 for customer and product dimensions
- Unknown member handling (SK = -1) for orphan facts
- Date dimension with fiscal calendar support

### Logging & Monitoring
- Structured pipeline logger writing to Delta Lake
- Step-level timing and row count tracking
- Success/failure/warning status per step
- Run summary aggregation

---

## Getting Started

### 1. Generate Some Test Data
```bash
cd retail-data-lakehouse
python scripts/generate_sample_data.py
```
This creates 30 days of fake sales data (~4,500+ records) with:
- 30 customers across 3 segments
- 20 products across 4 categories
- 8 stores in 8 US cities
- Intentionally messy data (~5%) for testing the cleaning logic
- Late-arriving records

### 2. Run Tests
```bash
pytest tests/ -v
```

### 3. Deploy to Azure

#### What You'll Need
- Azure subscription
- Azure Data Lake Storage Gen2 account
- Azure Databricks workspace
- Azure SQL Database
- Azure Data Factory instance

#### How to Set It Up
1. Copy `config/azure_config.template.json` to `config/azure_config.json` and fill in your details
2. Run `sql/create_tables.sql` on Azure SQL
3. Run `sql/stored_procedures.sql` on Azure SQL
4. Upload `databricks/` notebooks to Databricks workspace
5. Import `adf_pipelines/` JSON definitions into Data Factory
6. Upload generated CSV data to Data Lake `raw` container
7. Trigger `PL_Master_Orchestrator` pipeline

---

## Included Queries

| # | Query | What It Shows |
|---|-------|---------|
| 1 | Daily Revenue Summary | Revenue trends with weekend flag |
| 2 | Top 10 Products | Best sellers by revenue |
| 3 | Customer Segmentation | Segment-level spend analysis |
| 4 | Store Performance | Revenue + return rate comparison |
| 5 | Monthly Trend + MoM | Month-over-month growth |
| 6 | Category Rankings | Sub-category rank within category |
| 7 | Payment Methods | Payment mix analysis |
| 8 | Running Totals | Cumulative store revenue |
| 9 | Customer RFM | Recency/Frequency/Monetary scoring |
| 10 | Weekend vs Weekday | Day-type performance comparison |

---

## What This Demonstrates

| Skill | Implementation |
|-------|---------------|
| **ETL Pipeline** | Bronze → Silver → Gold medallion architecture |
| **Data Lake** | Azure Data Lake Gen2 with partitioned storage |
| **Lakehouse** | Delta Lake format across all layers |
| **Databricks** | PySpark notebooks with modular functions |
| **Data Modeling** | Star schema with fact + 4 dimension tables |
| **SCD Type 2** | Customer & product dimensions with temporal tracking |
| **Incremental Load** | Watermark-based delta processing |
| **Window Functions** | lag, lead, dense_rank, running totals, percent_rank |
| **Data Quality** | Profiling, quarantine, validation checks |
| **Azure Data Factory** | Parameterized pipelines, triggers, orchestration |
| **Azure SQL** | DDL, stored procedures, analytical queries |
| **Error Handling** | Structured logging, quarantine, retry patterns |
| **Testing** | pytest unit tests for quality rules & transforms |
| **Late-Arriving Data** | Configurable lookback window handling |

---

## Tech Stack

| Component | Technology |
|-----------|------------|
| Cloud Platform | Microsoft Azure |
| Data Lake | Azure Data Lake Storage Gen2 |
| Orchestration | Azure Data Factory |
| Processing | Azure Databricks (PySpark) |
| File Format | Delta Lake / Parquet |
| Database | Azure SQL Database |
| Language | Python 3.10+, SQL, PySpark |
| Testing | pytest |
