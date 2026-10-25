# Databricks notebook source
# MAGIC %md
# MAGIC ### Data Reading

# COMMAND ----------

df1=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/workspace/default/tutorial_learning/raw/customers.csv")

# COMMAND ----------

df1.display()

# COMMAND ----------

df2=spark.read.format("csv").option("header","true").option("inferSchema","true").load("/Volumes/workspace/default/tutorial_learning/raw/orders.csv")
df2.display()


# COMMAND ----------

df3=spark.read.format("csv").option("header",True).option("inferSchema",True).load("/Volumes/workspace/default/tutorial_learning/raw/order_items.csv")
df3.display()

# COMMAND ----------

# DBTITLE 1,Read products
df4=spark.read.format("csv").option("header",True).option("inferSchema",True).load("/Volumes/workspace/default/tutorial_learning/raw/products.csv")
df4.display()

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# DBTITLE 1,Order value rollup
# MAGIC %md
# MAGIC ### Order value rollup

# COMMAND ----------

df_ord_price=df3.groupBy('order_id').agg(round(sum(col('unit_price')*col('quantity')),0).alias("total_price"))
df_ord_price.display()

# COMMAND ----------

df_ord_price.printSchema()

# COMMAND ----------

# DBTITLE 1,Validate data types
# MAGIC %md
# MAGIC ###  Validate data types 

# COMMAND ----------

# DBTITLE 1,Customers schema
df1.printSchema()

# COMMAND ----------

df2.printSchema()

# COMMAND ----------

df3.printSchema()

# COMMAND ----------

df4.printSchema()

# COMMAND ----------

# DBTITLE 1,Check duplicates in all four tables
# MAGIC %md
# MAGIC ### Check duplicates in all four tables

# COMMAND ----------

df1.createOrReplaceTempView("customers")
df2.createOrReplaceTempView("orders")
df3.createOrReplaceTempView("order_items")
df4.createOrReplaceTempView("products")
df_ord_price.createOrReplaceTempView("order_price")


# COMMAND ----------

print("customers",df1.count())
print("orders",df2.count())
print("order_items",df3.count())
print("products",df4.count())

# COMMAND ----------

print("customers",df1.distinct().count())
print("orders",df2.distinct().count())
print("order_items",df3.distinct().count())
print("products",df4.distinct().count())

# COMMAND ----------

# DBTITLE 1,Duplicate records note
# MAGIC %md
# MAGIC #### 15 duplicate records in customers table

# COMMAND ----------

# MAGIC %sql
# MAGIC select customer_id,count(customer_id) from customers group by customer_id  having count(customer_id)>1

# COMMAND ----------

# DBTITLE 1,Joining all tables
# MAGIC %md
# MAGIC ### Joining all tables to get complete info

# COMMAND ----------

# DBTITLE 1,Join all tables (df)
# MAGIC %sql --name df
# MAGIC select 
# MAGIC   c.customer_id,
# MAGIC   c.customer_name,
# MAGIC   c.city,
# MAGIC   c.state,
# MAGIC   c.country,
# MAGIC   c.signup_date,
# MAGIC   o.order_id,
# MAGIC   o.order_date,
# MAGIC   o.payment_method,
# MAGIC   o.order_status,
# MAGIC   i.product_id,
# MAGIC   i.quantity,
# MAGIC   i.unit_price,
# MAGIC   p.product_name,
# MAGIC   p.category,
# MAGIC   p.price
# MAGIC from customers as c
# MAGIC left join orders as o on c.customer_id = o.customer_id
# MAGIC left join order_items as i on o.order_id = i.order_id
# MAGIC left join products as p on i.product_id = p.product_id
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Phase 1: Order and sales analysis

# COMMAND ----------

# DBTITLE 1,Order-level dataset
# MAGIC %md
# MAGIC #### Order-level dataset (orders + order value)

# COMMAND ----------

# DBTITLE 1,Order-level dataset (dfo)
# MAGIC %sql --name dfo
# MAGIC select o.*,op.total_price from orders as o left join order_price as op on o.order_id = op.order_id

# COMMAND ----------

dfo1=dfo.withColumn('month',month('order_date'))\
    .withColumn('year',year('order_date'))

# COMMAND ----------

dfo1.display()

# COMMAND ----------

# DBTITLE 1,Monthly Revenue
# MAGIC %md
# MAGIC #### Monthly Revenue

# COMMAND ----------

df_monthly=dfo1.filter(col('order_status').isin(['Shipped','Processing','Completed'])).groupby('year','month').agg(countDistinct('order_id').alias('total_orders'),sum('total_price').alias('total_revenue')).orderBy('year','month')


# COMMAND ----------

df_monthly.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Yearly Revenue

# COMMAND ----------

# DBTITLE 1,Yearly Revenue (excluding Cancelled/Returned)
df_yearly=dfo1.filter(col('year')<=2025).filter(col('order_status').isin(['Shipped','Processing','Completed'])).groupby('year').agg(sum('total_price').alias('total_revenue'),countDistinct('order_id').alias('total_orders')).orderBy('year')

# COMMAND ----------

df_yearly.withColumn('AOV',round(col('total_revenue')/col('total_orders'),2)).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Order Status Analysis

# COMMAND ----------

df.groupby('order_status').agg(count('customer_id').alias('count')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Payment method analysis

# COMMAND ----------

dfo1.groupby('payment_method').agg(countDistinct('order_id').alias('total_orders'),sum('total_price').alias('total_revenue')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Phase 2: Product analysis

# COMMAND ----------

# DBTITLE 1,Product-level dataset (dfo2)
# MAGIC %sql --name dfo2
# MAGIC select oi.*,o.order_status,p.product_name,p.category,p.price from order_items as oi left join orders as o on oi.order_id = o.order_id left join products as p on oi.product_id = p.product_id

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top 10 products by revenue

# COMMAND ----------

dfo2.filter(col('order_status').isin(['Shipped','Processing','Completed'])).groupby('product_name').agg(sum('quantity').alias('total_quantity'),round(sum(col('quantity')*col('unit_price')),0).alias('total_revenue')).orderBy(col('total_revenue').desc()).limit(10).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Category performance

# COMMAND ----------

from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Category performance
dfo2.filter(col('order_status').isin(['Shipped','Processing','Completed'])).groupby('category').agg(sum('quantity').alias('total_quantity'),countDistinct('order_id').alias('total_orders'),round(sum(col('quantity')*col('unit_price')),0).alias('total_revenue')).withColumn('ranking',rank().over(Window.partitionBy().orderBy(col('total_revenue').desc()))).display()


# COMMAND ----------

# MAGIC %md
# MAGIC ### Phase 3: Customer analysis

# COMMAND ----------

# DBTITLE 1,Joined data overview
# MAGIC %md
# MAGIC #### Overview of the joined data

# COMMAND ----------

df.display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Top customers by spending

# COMMAND ----------

df.filter(col('order_status').isin(['Shipped','Processing','Completed'])).groupby('customer_id','customer_name').agg(countDistinct('order_id').alias('total_orders'),round(sum((col('quantity')*col('unit_price'))),2).alias('total_spent')).orderBy(col('total_spent').desc()).limit(10).display()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Sales by state or city

# COMMAND ----------

df.filter(col('order_status').isin(['Shipped','Processing','Completed'])).groupby('state').agg(countDistinct('order_id').alias('total_orders'),sum('quantity').alias('total_quantity'),round(sum((col('quantity')*col('unit_price'))),0).alias('total_revenue')).orderBy(col('total_revenue').desc()).display()

# COMMAND ----------

df.filter(col('order_status').isin(['Shipped','Processing','Completed'])).groupby('city').agg(countDistinct('order_id').alias('total_orders'),sum('quantity').alias('total_quantity'),round(sum((col('quantity')*col('unit_price'))),0).alias('total_revenue')).orderBy(col('total_revenue').desc()).display()

# COMMAND ----------

# DBTITLE 1,Phase 4: Advanced analysis
# MAGIC %md
# MAGIC ### Phase 4: Advanced analysis

# COMMAND ----------

# MAGIC %md
# MAGIC #### Cancelled orders and potential lost sales

# COMMAND ----------

df.filter(col('order_status').isin(['Cancelled','Completed'])).groupby('order_status').agg(countDistinct('order_id').alias('total_orders'),round(sum((col('quantity')*col('unit_price'))),0).alias('total_revenue')).display()


# COMMAND ----------

# MAGIC %md
# MAGIC #### Customer signup cohort

# COMMAND ----------

# MAGIC %sql --name df_info
# MAGIC select c.*,order_id,o.order_date,o.payment_method,o.order_status from customers as c left join orders as o on c.customer_id = o.customer_id

# COMMAND ----------

# DBTITLE 1,Signup cohort: month and year columns
df_info=df_info.withColumn('signup_month',month('signup_date'))\
        .withColumn('signupyear',year('signup_date'))

# COMMAND ----------

df_info.groupby('signupyear','signup_month').agg(countDistinct('customer_id').alias('total_customers')).orderBy('signupyear','signup_month').display()


# COMMAND ----------

df_info.groupby('signupyear', 'signup_month').agg(
    countDistinct('customer_id').alias('total_customers'),
    countDistinct(when(col('order_id').isNotNull(), col('customer_id'))).alias('customers_who_ordered')
).withColumn(
    'conversion_rate',
    round(col('customers_who_ordered') / col('total_customers') * 100, 2)
).orderBy('signupyear', 'signup_month').display()

# COMMAND ----------

# DBTITLE 1,README
# MAGIC %md
# MAGIC # 🛒 Ecommerce Sales Analytics — PySpark ETL Project
# MAGIC
# MAGIC > End-to-end **ETL + analytics pipeline** on Databricks: raw e-commerce CSVs → validated & joined
# MAGIC > DataFrames → four phases of business analysis (sales, products, customers, cohorts).
# MAGIC
# MAGIC ---
# MAGIC
# MAGIC ## 📦 Dataset
# MAGIC
# MAGIC Raw CSVs loaded from the Unity Catalog volume `workspace.default.tutorial_learning`:
# MAGIC
# MAGIC | File | Grain | Rows | Key Columns |
# MAGIC |---|---|---:|---|
# MAGIC | `customers.csv` | one row per customer record | 2,015 *(2,000 distinct)* | `customer_id`, `customer_name`, `city`, `state`, `country`, `signup_date` |
# MAGIC | `orders.csv` | one row per order | 10,000 | `order_id`, `customer_id`, `order_date`, `payment_method`, `order_status` |
# MAGIC | `order_items.csv` | one row per product per order | 21,968 | `order_id`, `product_id`, `quantity`, `unit_price` |
# MAGIC | `products.csv` | one row per product | 100 | `product_id`, `product_name`, `category`, `price` |
# MAGIC
# MAGIC **Coverage** — signups Jan 2022 → Jun 2024 · orders Jan 2022 → Jan 2026 · all customers in India · 7 product categories · 5 payment methods.
# MAGIC
# MAGIC ## 🔁 Pipeline
# MAGIC
# MAGIC ```
# MAGIC UC Volume (CSV) ─▶ Spark DataFrames ─▶ schema validation (printSchema)
# MAGIC                                    └▶ duplicate audit  →  ⚠ 15 duplicate rows in customers
# MAGIC       ├─▶ order-level value rollup:  Σ (quantity × unit_price)  per order_id
# MAGIC       └─▶ star join via SQL temp views:  customers ⋈ orders ⋈ order_items ⋈ products
# MAGIC                      │
# MAGIC                      ▼
# MAGIC      Phase 1: Sales  ·  Phase 2: Products  ·  Phase 3: Customers  ·  Phase 4: Advanced
# MAGIC ```
# MAGIC
# MAGIC **Data-quality rules applied**
# MAGIC * Revenue metrics count only `Shipped`, `Processing`, `Completed` orders — `Cancelled` / `Returned` are excluded.
# MAGIC * Yearly totals are capped at `year ≤ 2025` (a handful of orders fall in Jan 2026).
# MAGIC
# MAGIC ## 📊 Analysis Phases
# MAGIC
# MAGIC | Phase | Sections | Questions answered |
# MAGIC |---|---|---|
# MAGIC | **1 — Orders & Sales** | Monthly revenue · Yearly revenue + AOV · Order status · Payment methods | How do orders and revenue trend over time? |
# MAGIC | **2 — Products** | Top 10 products · Category performance with `rank()` window | Which products and categories drive revenue? |
# MAGIC | **3 — Customers** | Top spenders · Sales by state & city | Who and where are the best customers? |
# MAGIC | **4 — Advanced** | Cancelled orders & lost sales · Signup cohorts + conversion rate | How much revenue leaks to cancellations? Do cohorts convert? |
# MAGIC
# MAGIC ## 💡 Key Insights
# MAGIC
# MAGIC **Yearly performance** (valid orders, year ≤ 2025):
# MAGIC
# MAGIC | Year | Orders | Revenue | AOV |
# MAGIC |---|---:|---:|---:|
# MAGIC | 2022 | 440 | $4.3M | $9.7K |
# MAGIC | 2023 | 1,564 | $15.9M | $10.2K |
# MAGIC | 2024 | 3,162 | $32.5M | $10.3K |
# MAGIC | 2025 | 3,363 | $39.0M | $11.6K |
# MAGIC
# MAGIC * 📈 **~9× revenue growth** from 2022 to 2025, with AOV rising from ~$9.7K to ~$11.6K.
# MAGIC * 🖥️ **Electronics dominates** — $63.6M ≈ **67%** of product revenue; next category (Clothing) is ~$9.8M.
# MAGIC * 📱 **UPI is the #1 payment method** — 3,823 orders (~44% of valid orders) and $40.7M revenue.
# MAGIC * 🛑 **~$12.4M potential lost sales** — 690 cancelled orders ($7.5M) + 507 returned orders ($4.9M).
# MAGIC * 🧑‍🤝‍🧑 **99.4% conversion** — 1,987 of 2,000 customers placed at least one order.
# MAGIC * 🗺️ **Gujarat is the top state** ($11.1M), followed by Uttar Pradesh, Maharashtra, Punjab, and West Bengal.
# MAGIC
# MAGIC ## 🧰 Tech Stack
# MAGIC
# MAGIC * **Databricks Serverless** compute
# MAGIC * **PySpark** DataFrame API — `filter`, `groupBy().agg()`, `countDistinct`, `when`, `withColumn`
# MAGIC * **Spark SQL** — temp views + `--name` result DataFrames for joins and cohort analysis
# MAGIC * **Window functions** — `rank() over Window.partitionBy() ... orderBy()`
# MAGIC * **Unity Catalog Volumes** for raw-file storage
# MAGIC
# MAGIC ## 🚀 How to Run
# MAGIC
# MAGIC 1. Attach a **Serverless** compute to this notebook.
# MAGIC 2. Run all cells top-to-bottom — every step rebuilds from the raw CSVs.
# MAGIC 3. Requires read access to `/Volumes/workspace/default/tutorial_learning/raw/`.
# MAGIC
# MAGIC ## ⚠ Data Quality Notes
# MAGIC
# MAGIC * 15 duplicate rows in `customers` (2,015 rows vs 2,000 distinct IDs) — flagged during the duplicate audit, not dropped.
# MAGIC * ~$0.4M of revenue maps to a `NULL` category — order items referencing products missing from `products.csv`.
# MAGIC * 52 orders have a `NULL` payment method (~$0.5M revenue).
# MAGIC