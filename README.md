E-Commerce Data Pipeline

An end-to-end cloud-native data pipeline that ingests live e-commerce data 
from a REST API, transforms it using AWS Glue, stores it in S3 as Parquet, 
and loads it into Snowflake for analytics.

---

##  Architecture

```
DummyJSON API
     ↓
AWS S3 (Raw Zone)
     ↓
AWS Glue (PySpark ETL)
     ↓
AWS S3 (Curated Zone - Parquet)
     ↓
Snowflake (Data Warehouse)
     ↓
Analytics Views (SQL)
```

---

##  Tech Stack

| Layer | Tool |
|---|---|
| Data Source | DummyJSON REST API |
| Ingestion | Python (requests) |
| Processing | AWS Glue (PySpark) |
| Raw Storage | AWS S3 |
| Curated Storage | AWS S3 (Parquet + Snappy) |
| Data Warehouse | Snowflake |
| Auth | AWS IAM Role + Snowflake Storage Integration |
| Version Control | GitHub |

---

## Project Structure

```
ecom-data-pipeline/
├── glue_jobs/
│   ├── ingestion.py          # API fetch + S3 raw upload
│   └── glue_etl_job.py       # PySpark clean + Parquet write
├── sql/
│   ├── setup/
│   │   ├── 01_snowflake_setup.sql
│   │   ├── 02_storage_integration.sql
│   │   ├── 03_stage_setup.sql
│   │   └── 04_orders_table.sql
│   └── views/
│       └── analytics_views.sql
└── README.md
```

---

## Data Pipeline Stages

### Stage 1 — Ingestion
- Fetches live data from DummyJSON API (`/users`, `/products`, `/carts`)
- Enriches orders with customer details
- Calculates `revenue`, `cost_price`, `profit`
- Uploads raw JSON to S3 partitioned by date

### Stage 2 — Transformation (AWS Glue)
- Reads raw JSON from S3
- Cleans nulls, empty strings, duplicates
- Writes cleaned Parquet (Snappy compressed) to curated S3 zone
- Partitioned by `order_date`

### Stage 3 — Loading (Snowflake)
- Storage integration via AWS IAM Role trust policy
- External stage pointing to curated S3 bucket
- `COPY INTO` loads Parquet into `orders` table

### Stage 4 — Analytics Views
| View | Purpose |
|---|---|
| `vw_sales_by_category` | Revenue & profit per category |
| `vw_top_customers` | Top customers by revenue |
| `vw_order_status` | Orders by status |
| `vw_sales_by_country` | Revenue by country |
| `vw_daily_revenue` | Daily revenue trend |

---

## Sample Output (Feb 18, 2026)

| Metric | Value |
|---|---|
| Total Orders | 198 |
| Total Revenue | $1,004,267.86 |
| Total Profit | $401,709.72 |
| Profit Margin | 40% |

---

##  How to Run

### 1. Set Up AWS
- Create S3 buckets: `ecom-raw-zone` and `ecom-curated-zone`
- Create IAM Role `SnowflakeS3Role` with S3 read permissions
- Deploy Glue job using `glue_jobs/glue_etl_job.py`

### 2. Set Up Snowflake
```sql
-- Run in order:
-- sql/setup/01_snowflake_setup.sql
-- sql/setup/02_storage_integration.sql
-- sql/setup/03_stage_setup.sql
-- sql/setup/04_orders_table.sql
-- sql/views/analytics_views.sql
```

### 3. Run Ingestion
```bash
python glue_jobs/ingestion.py
```

---

## Security Notes

- Never commit AWS credentials or Snowflake passwords
- Use environment variables or AWS Secrets Manager
- IAM Role ARNs are project-specific — update before reuse

---

