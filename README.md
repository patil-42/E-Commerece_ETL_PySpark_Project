# 🛒 Ecommerce Sales Analytics — PySpark ETL Project

Hey! This is my end-to-end ETL + analytics project on Databricks. I start with raw e-commerce CSVs, clean them up, join all four tables, run four phases of business analysis, and save everything as Delta tables in Unity Catalog. Everything runs on Serverless compute — no cluster setup needed.

---

## 📦 What Data I'm Working With

I loaded four raw CSVs from a Unity Catalog volume:

| File | What's in it | Rows | Key Columns |
|---|---|---:|---|
| customers.csv | one row per customer | 2,015 *(2,000 unique)* | customer_id, customer_name, city, state, country, signup_date |
| orders.csv | one row per order | 10,000 | order_id, customer_id, order_date, payment_method, order_status |
| order_items.csv | one row per product per order | 21,968 | order_id, product_id, quantity, unit_price |
| products.csv | one row per product | 100 | product_id, product_name, category, price |

The data covers customer signups from Jan 2022 to Jun 2024, orders from Jan 2022 to Jan 2026, all customers in India, 7 product categories, and 5 payment methods.

---

## 🔁 How My Pipeline Works

```
Raw CSVs (UC Volume)
      ↓
Load into Spark DataFrames → check schemas (printSchema)
      ↓
Audit for duplicates → found 15 duplicate customers → cleaned them up
      ↓
Roll up order items to get one total price per order
      ↓
Join all 4 tables with SQL temp views (customers ⋈ orders ⋈ order_items ⋈ products)
      ↓
Phase 1: Sales     →  Phase 2: Products  →  Phase 3: Customers  →  Phase 4: Advanced
      ↓
Phase 5: Save everything as Delta tables (Bronze / Silver / Gold)
```

**My data-quality rules:**
* I only count Shipped, Processing, and Completed orders as revenue — Cancelled and Returned ones are excluded.
* I cap yearly totals at 2025 (a few orders spill into Jan 2026 which I don't want to include).

---

## 📊 What I Analyzed

| Phase | What I looked at | What I wanted to find out |
|---|---|---|
| **1 — Sales** | Monthly revenue, yearly revenue + AOV, order status, payment methods | How are orders and revenue trending over time? |
| **2 — Products** | Top 10 products, category ranking using rank() window function | Which products and categories bring in the most money? |
| **3 — Customers** | Top spenders, sales by state and city | Who are my best customers and where are they? |
| **4 — Advanced** | Cancelled orders & lost sales, signup cohorts + conversion rate | How much money am I losing to cancellations? Do my cohorts actually convert? |

---

## 💡 Key Findings

**Revenue grew about 9x in four years:**

| Year | Orders | Revenue | Avg Order Value |
|---|---:|---:|---:|
| 2022 | 440 | $4.3M | $9.7K |
| 2023 | 1,564 | $15.9M | $10.2K |
| 2024 | 3,162 | $32.5M | $10.3K |
| 2025 | 3,363 | $39.0M | $11.6K |

Here's what stood out to me:

* 📈 **Revenue grew ~9x** from 2022 to 2025, and the average order value went from ~$9.7K to ~$11.6K — people are spending more per order.
* 🖥️ **Electronics is the king** — $63.6M, about 67% of all product revenue. The next category (Clothing) is way behind at ~$9.8M.
* 📱 **UPI dominates payments** — 3,823 orders (about 44% of all valid orders) and $40.7M in revenue. UPI is clearly the preferred way to pay.
* 🛑 **~$12.4M in lost sales** — 690 cancelled orders ($7.5M) plus 507 returned orders ($4.9M). That's a big chunk of money walking out the door.
* 🧑‍🤝‍🧑 **99.4% conversion rate** — 1,987 out of 2,000 customers actually placed an order. Almost everyone who signed up bought something.
* 🗺️ **Gujarat is the top state** ($11.1M revenue), followed by Uttar Pradesh, Maharashtra, Punjab, and West Bengal.

---

## 🏅 Phase 5 — Medallion Architecture (Delta Tables)

I saved all my pipeline results as real Delta tables in Unity Catalog so they stick around and any dashboard or query can use them:

| Layer | Tables | What's in there |
|---|---|---|
| 🥉 Bronze | bronze_customers, bronze_orders, bronze_order_items, bronze_products | Raw data exactly as I got it (duplicates kept for audit) |
| 🥈 Silver | silver_customers, silver_orders, silver_order_items, silver_products | Cleaned up — removed duplicates, validated everything (2,000 customers) |
| 🥇 Gold | gold_monthly_revenue, gold_yearly_revenue, gold_top_products, gold_customer_spend, gold_state_sales | Final business numbers — ready for dashboards |

---

## 🧰 Tech I Used

* **Databricks Serverless** compute — no cluster setup needed
* **PySpark** DataFrame API — filter, groupBy, agg, countDistinct, when, withColumn
* **Spark SQL** — temp views for joining tables and cohort analysis
* **Window functions** — rank() for category performance ranking
* **Unity Catalog Volumes** — for storing raw CSV files
* **Delta Lake** — Bronze/Silver/Gold medallion architecture in Unity Catalog

---

## 🚀 How to Run This

1. Attach a **Serverless** compute to the notebook.
2. Run all cells top to bottom — everything rebuilds from the raw CSVs.
3. You need read access to `/Volumes/workspace/default/tutorial_learning/raw/`.
4. Phase 5 writes 13 Delta tables to the `workspace.default` schema, so you need CREATE permission there.

---

## ⚠️ Data Quality Notes

* I found 15 duplicate rows in customers (2,015 rows but only 2,000 unique customer IDs). I flagged them in Bronze, then removed them in Silver.
* About $0.4M of revenue maps to a NULL category — some order items reference products that don't exist in products.csv.
* 52 orders have a NULL payment method (~$0.5M revenue) — minor data gaps I noted but didn't fix.

---

## 👋 About This Project

I built this as a hands-on PySpark and Spark SQL learning project on Databricks. It covers the full journey — from raw CSVs to clean data, business analysis, and a proper medallion architecture in Delta Lake. Everything runs on Serverless, so there's no cluster to manage.
