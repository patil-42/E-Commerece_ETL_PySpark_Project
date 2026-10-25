# E-Commerce PySpark ETL Project

A complete end-to-end ETL pipeline built in **PySpark** and **Spark SQL** on Databricks, processing e-commerce data from raw CSV files through to analytical insights.

## Project Overview

This project demonstrates a full ETL workflow on an e-commerce dataset covering **4 tables**: customers, orders, order items, and products.

### Data Source
- **Location**: Unity Catalog Volume (`/Volumes/workspace/default/tutorial_learning/raw/`)
- **Format**: CSV (4 files: `customers.csv`, `orders.csv`, `order_items.csv`, `products.csv`)
- **Dataset Stats**:
  - Customers: 2,015 rows / 2,000 distinct IDs (15 duplicates found)
  - Orders: 10,000 rows (Jan 2022 – Jan 2026)
  - Order Items: 21,968 rows
  - Products: 100 rows
  - All customers based in India

### Pipeline Structure

| Stage | Description |
| --- | --- |
| **Data Reading** | Load 4 CSVs from UC Volume with schema inference |
| **Order Value Rollup** | Compute order-level metrics (total price, item counts) |
| **Schema Validation** | Validate data types across all tables |
| **Duplicate Audit** | Identify and audit duplicate records |
| **Table Joins** | Join all 4 tables using SQL temp views |
| **Phase 1: Sales Analysis** | Revenue trends, yearly growth ($4.3M → $39.0M), payment methods |
| **Phase 2: Product Analysis** | Category performance (Electronics ≈ 67% of revenue) |
| **Phase 3: Customer Analysis** | Conversion rate (99.4%), state-wise distribution (Gujarat top) |
| **Phase 4: Advanced/Cohort** | Cancelled/returned revenue (~$12.4M), cohort analysis |

### Key Insights
- Valid revenue grew from **$4.3M (2022)** to **$39.0M (2025)**
- **Electronics** dominates with ~67% of total revenue
- **UPI** is the top payment method
- ~**$12.4M** in cancelled + returned revenue
- **99.4%** customer conversion rate
- **Gujarat** is the top state by customer count

## Tech Stack

- **Compute**: Databricks Serverless
- **Language**: PySpark + Spark SQL
- **Data Source**: Unity Catalog Volumes (CSV)
- **Platform**: Databricks

## Notebook

The main notebook `Ecommerce_PySpark_ETL_Project.py` contains the complete ETL pipeline with 62 cells covering all stages from data ingestion to advanced analytics.

## How to Run

1. Import the notebook into a Databricks workspace
2. Ensure the source CSV files are available in a Unity Catalog volume
3. Run on Serverless compute (or any compatible cluster)
4. Execute cells sequentially from top to bottom

## Author

Built as a PySpark/Spark SQL learning project on Databricks.