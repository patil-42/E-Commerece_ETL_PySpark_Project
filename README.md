# 🛒 Ecommerce Sales Analytics — PySpark ETL Project

> End-to-end **ETL + analytics pipeline** on Databricks: raw e-commerce CSVs → validated & joined
> DataFrames → four phases of business analysis (sales, products, customers, cohorts).

---

## 📦 Dataset

Raw CSVs loaded from the Unity Catalog volume `workspace.default.tutorial_learning`:

| File | Grain | Rows | Key Columns |
|---|---|---:|---|
| `customers.csv` | one row per customer record | 2,015 *(2,000 distinct)* | `customer_id`, `customer_name`, `city`, `state`, `country`, `signup_date` |
| `orders.csv` | one row per order | 10,000 | `order_id`, `customer_id`, `order_date`, `payment_method`, `order_status` |
| `order_items.csv` | one row per product per order | 21,968 | `order_id`, `product_id`, `quantity`, `unit_price` |
| `products.csv` | one row per product | 100 | `product_id`, `product_name`, `category`, `price` |

**Coverage** — signups Jan 2022 → Jun 2024 · orders Jan 2022 → Jan 2026 · all customers in India · 7 product categories · 5 payment methods.

## 🔁 Pipeline

```
UC Volume (CSV) ─▶ Spark DataFrames ─▶ schema validation (printSchema)
                                   └▶ duplicate audit  →  ⚠ 15 duplicate rows in customers
      ├─▶ order-level value rollup:  Σ (quantity × unit_price)  per order_id
      └─▶ star join via SQL temp views:  customers ⋈ orders ⋈ order_items ⋈ products
                     │
                     ▼
     Phase 1: Sales  ·  Phase 2: Products  ·  Phase 3: Customers  ·  Phase 4: Advanced
```

**Data-quality rules applied**
* Revenue metrics count only `Shipped`, `Processing`, `Completed` orders — `Cancelled` / `Returned` are excluded.
* Yearly totals are capped at `year ≤ 2025` (a handful of orders fall in Jan 2026).

## 📊 Analysis Phases

| Phase | Sections | Questions answered |
|---|---|---|
| **1 — Orders & Sales** | Monthly revenue · Yearly revenue + AOV · Order status · Payment methods | How do orders and revenue trend over time? |
| **2 — Products** | Top 10 products · Category performance with `rank()` window | Which products and categories drive revenue? |
| **3 — Customers** | Top spenders · Sales by state & city | Who and where are the best customers? |
| **4 — Advanced** | Cancelled orders & lost sales · Signup cohorts + conversion rate | How much revenue leaks to cancellations? Do cohorts convert? |

## 💡 Key Insights

**Yearly performance** (valid orders, year ≤ 2025):

| Year | Orders | Revenue | AOV |
|---|---:|---:|---:|
| 2022 | 440 | $4.3M | $9.7K |
| 2023 | 1,564 | $15.9M | $10.2K |
| 2024 | 3,162 | $32.5M | $10.3K |
| 2025 | 3,363 | $39.0M | $11.6K |

* 📈 **~9× revenue growth** from 2022 to 2025, with AOV rising from ~$9.7K to ~$11.6K.
* 🖥️ **Electronics dominates** — $63.6M ≈ **67%** of product revenue; next category (Clothing) is ~$9.8M.
* 📱 **UPI is the #1 payment method** — 3,823 orders (~44% of valid orders) and $40.7M revenue.
* 🛑 **~$12.4M potential lost sales** — 690 cancelled orders ($7.5M) + 507 returned orders ($4.9M).
* 🧑‍🤝‍🧑 **99.4% conversion** — 1,987 of 2,000 customers placed at least one order.
* 🗺️ **Gujarat is the top state** ($11.1M), followed by Uttar Pradesh, Maharashtra, Punjab, and West Bengal.

## 🧰 Tech Stack

* **Databricks Serverless** compute
* **PySpark** DataFrame API — `filter`, `groupBy().agg()`, `countDistinct`, `when`, `withColumn`
* **Spark SQL** — temp views + `--name` result DataFrames for joins and cohort analysis
* **Window functions** — `rank() over Window.partitionBy() ... orderBy()`
* **Unity Catalog Volumes** for raw-file storage

## 🚀 How to Run

1. Attach a **Serverless** compute to this notebook.
2. Run all cells top-to-bottom — every step rebuilds from the raw CSVs.
3. Requires read access to `/Volumes/workspace/default/tutorial_learning/raw/`.

## ⚠ Data Quality Notes

* 15 duplicate rows in `customers` (2,015 rows vs 2,000 distinct IDs) — flagged during the duplicate audit, not dropped.
* ~$0.4M of revenue maps to a `NULL` category — order items referencing products missing from `products.csv`.
* 52 orders have a `NULL` payment method (~$0.5M revenue).


## Author

Built as a PySpark/Spark SQL learning project on Databricks.
