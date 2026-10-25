# Databricks Quick Start Guide

A practical intro to the Databricks platform with the most common things you'll do day-to-day.

---

## Table of Contents
1. [What is Databricks?](#1-what-is-databricks)
2. [Workspace & Notebooks](#2-workspace--notebooks)
3. [Running Code](#3-running-code)
4. [Reading & Writing Data](#4-reading--writing-data)
5. [PySpark Basics](#5-pyspark-basics)
6. [Spark SQL Basics](#6-spark-sql-basics)
7. [Visualizing Data](#7-visualizing-data)
8. [Unity Catalog (Tables, Schemas, Volumes)](#8-unity-catalog)
9. [Git Integration](#9-git-integration)
10. [Scheduling Jobs](#10-scheduling-jobs)
11. [Dashboards](#11-dashboards)
12. [Tips & Shortcuts](#12-tips--shortcuts)

---

## 1. What is Databricks?

Databricks is a cloud-based unified analytics platform built on Apache Spark. It combines:

- **Notebooks** for interactive coding (Python, SQL, Scala, R)
- **Spark** for distributed data processing
- **Unity Catalog** for data governance & security
- **Jobs** for scheduling & automation
- **Dashboards** for BI & reporting
- **MLflow** for machine learning tracking

You write code in notebooks, run it on compute clusters (Serverless or Classic), and work with data stored in Unity Catalog.

---

## 2. Workspace & Notebooks

### Creating a Notebook
1. Click **+ New** in the sidebar → **Notebook**
2. Name it, pick a default language (Python, SQL, Scala, R)
3. Choose or let it use your current compute

### Cell Types
| Type | Purpose | How to create |
|---|---|---|
| **Code cell** | Run Python/SQL/Scala/R | Default cell type |
| **Markdown cell** | Add headings, notes, documentation | Change cell language to Markdown, or start with `%md` |

### Cell Operations
| Action | Shortcut (Mac) | Shortcut (Windows) |
|---|---|---|
| Run current cell | `Shift+Enter` | `Shift+Enter` |
| Run all above | `Cmd+Shift+A` | `Ctrl+Shift+A` |
| Run all below | `Cmd+Shift+B` | `Ctrl+Shift+B` |
| Add cell below | `B` (when cell selected) | `B` |
| Add cell above | `A` (when cell selected) | `A` |
| Delete cell | `D,D` (double-tap D) | `D,D` |
| Undo delete | `Z` | `Z` |
| Toggle comment | `Cmd+/` | `Ctrl+/` |

### Changing Cell Language
- Type `%python` at the top of a cell → Python
- Type `%sql` at the top of a cell → SQL
- Type `%md` at the top of a cell → Markdown
- Type `%scala` at the top of a cell → Scala
- Type `%r` at the top of a cell → R

This lets you mix languages in the same notebook.

---

## 3. Running Code

### Compute Types
| Type | When to use |
|---|---|
| **Serverless** | Fast start, no cluster management, pay-per-use. Best for most interactive work. |
| **Classic cluster** | Long-running sessions, custom configs, GPU workloads. |

### Running a Single Cell
- Press `Shift+Enter` to run the current cell and move to the next
- Press `Ctrl+Enter` to run the current cell and stay on it

### Running Selected Text
- Highlight part of a cell → click **Run Selected Text** (or use the play dropdown)

### Running All Cells
- Click the **Run All** button at the top of the notebook

### Clearing Outputs
- Click **Run** dropdown → **Clear all outputs**

---

## 4. Reading & Writing Data

### Reading CSV from a Unity Catalog Volume
```python
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Volumes/catalog/schema/volume/filename.csv")
```

### Reading CSV from workspace files
```python
df = spark.read.format("csv") \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .load("/Workspace/Users/your_email/path/file.csv")
```

### Reading a Delta table
```python
df = spark.read.table("catalog.schema.table_name")
# or
df = spark.sql("SELECT * FROM catalog.schema.table_name")
```

### Writing data to a Delta table
```python
df.write \
    .mode("overwrite") \
    .saveAsTable("catalog.schema.table_name")
```

### Writing to a Delta file path
```python
df.write \
    .mode("overwrite") \
    .format("delta") \
    .save("/Volumes/catalog/schema/volume/output_folder")
```

### Common write modes
| Mode | Behavior |
|---|---|
| `overwrite` | Replace existing data entirely |
| `append` | Add new data to existing |
| `ignore` | Do nothing if data already exists |
| `errorifexists` | (default) Error if data exists |

---

## 5. PySpark Basics

### Creating a DataFrame
```python
from pyspark.sql.functions import *

df = spark.read.csv("path", header=True, inferSchema=True)
```

### Viewing Data
```python
df.display()       # Interactive table view (Databricks-specific)
df.show()           # Plain text table
df.printSchema()   # Print column names and types
df.count()         # Row count
df.columns          # List of column names
df.describe().display()  # Summary statistics
```
> **Always use `df.display()` instead of `df.show()` in Databricks** — it gives a rich interactive table.

### Selecting & Filtering
```python
# Select specific columns
df.select("col1", "col2").display()

# Filter rows
df.filter(col("status") == "Shipped").display()
df.filter("status = 'Shipped'").display()  # SQL-style string filter

# Multiple conditions
df.filter((col("status") == "Shipped") & (col("amount") > 1000)).display()
```

### Adding / Transforming Columns
```python
df = df.withColumn("total", col("price") * col("quantity"))
df = df.withColumn("year", year(col("date")))
df = df.withColumn("month", month(col("date")))
df = df.withColumnRenamed("old_name", "new_name")
```

### Grouping & Aggregating
```python
from pyspark.sql.functions import sum, count, countDistinct, avg, round, col

df.groupBy("category") \
  .agg(
    count("id").alias("total_count"),
    sum("revenue").alias("total_revenue"),
    countDistinct("customer_id").alias("unique_customers")
  ) \
  .display()
```

### Ordering & Limiting
```python
df.orderBy(col("revenue").desc()).limit(10).display()
```

### Joins
```python
joined = df1.join(df2, df1["id"] == df2["customer_id"], "left")
# Join types: "inner", "left", "right", "outer", "cross"
```

### Creating Temp Views (use in SQL cells)
```python
df.createOrReplaceTempView("my_table")
```
Then in a SQL cell:
```sql
SELECT * FROM my_table WHERE status = 'Shipped'
```

### Common Functions
```python
from pyspark.sql.functions import *

# String functions
upper(col("name")), lower(col("name")), trim(col("name")), length(col("name"))
substring(col("name"), 1, 3), concat(col("first"), col("last"))
split(col("name"), " "), regexp_replace(col("name"), "old", "new")

# Date functions
current_date(), current_timestamp(), year(col("date")), month(col("date")), day(col("date"))
date_add(col("date"), 7), date_diff(col("end"), col("start"))
to_date(col("string_date"), "yyyy-MM-dd")

# Math functions
round(col("value"), 2), abs(col("value")), ceil(col("value")), floor(col("value"))

# Conditional
when(col("status") == "Shipped", "Done").otherwise("Pending")
```

---

## 6. Spark SQL Basics

### Using SQL Cells
Add `%sql` at the top of a cell, then write standard SQL:
```sql
%sql
SELECT * FROM catalog.schema.table_name LIMIT 10
```

### Naming a SQL Result (Databricks-specific)
```sql
%sql --name my_result
SELECT * FROM orders WHERE status = 'Completed'
```
This creates a Python variable `my_result` you can use in later cells:
```python
my_result.display()
```

### Common SQL Patterns
```sql
-- Aggregate
SELECT category, COUNT(*) as count, SUM(revenue) as total
FROM my_table
GROUP BY category
ORDER BY total DESC

-- Join
SELECT c.name, o.order_id, o.total
FROM customers c
LEFT JOIN orders o ON c.customer_id = o.customer_id

-- Window function
SELECT *,
  RANK() OVER (PARTITION BY category ORDER BY revenue DESC) as rank
FROM products

-- CTE
WITH high_value AS (
  SELECT * FROM orders WHERE total > 10000
)
SELECT * FROM high_value ORDER BY total DESC
```

---

## 7. Visualizing Data

### Using display() Built-in Charts
1. Run a cell with `df.display()`
2. Click the **+** next to the table → choose a chart type (bar, line, pie, scatter, etc.)
3. Pick X and Y axes → the chart renders inline

### Using Matplotlib
```python
import matplotlib.pyplot as plt

pdf = df.toPandas()
plt.figure(figsize=(10, 5))
plt.bar(pdf["category"], pdf["revenue"])
plt.xlabel("Category")
plt.ylabel("Revenue")
plt.title("Revenue by Category")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
```

### Using createVisualization (Databricks)
After running a cell with results, use the Databricks visualization tool to create charts that persist in the notebook.

---

## 8. Unity Catalog

Unity Catalog is Databricks' data governance layer. Everything is organized as:
```
Catalog
  └── Schema (database)
        └── Table
        └── Volume (file storage)
```

### Common Operations
```sql
-- Create a table
CREATE TABLE catalog.schema.my_table (id INT, name STRING);

-- Create a schema
CREATE SCHEMA catalog.my_schema;

-- List tables
SHOW TABLES IN catalog.schema;

-- Describe a table
DESCRIBE TABLE catalog.schema.table_name;

-- Drop a table
DROP TABLE catalog.schema.table_name;
```

### Volumes (File Storage)
Volumes store raw files (CSV, JSON, Parquet, images, etc.) in Unity Catalog.
```python
# List files in a volume
dbutils.fs.ls("/Volumes/catalog/schema/volume/")

# Read from volume
df = spark.read.csv("/Volumes/catalog/schema/volume/file.csv", header=True)
```

### Checking Permissions
```sql
SHOW GRANTS ON catalog.schema.table_name;
```

---

## 9. Git Integration

### Connecting Your GitHub/GitLab Account
1. Go to **Settings** → **Linked Accounts**
2. Connect your Git provider (GitHub, GitLab, Bitbucket, etc.)

### Cloning a Repo
1. Click **Repos** in the sidebar
2. Click **Add Repo** → paste your Git URL
3. The repo appears as a folder in your workspace

### Commit & Push
1. Make changes in the notebook inside the Git folder
2. Click the Git icon or ask in chat: "commit and push"
3. Write a commit message → push to remote

### Pull Changes
- Click **Pull** in the Git folder, or ask: "pull latest changes"

### Best Practices
- Keep notebooks in Git folders for version control
- Use `.gitignore` to exclude large data files when needed
- Commit frequently with descriptive messages

---

## 10. Scheduling Jobs

### Creating a Scheduled Job
1. Click **Jobs** in the sidebar → **Create Job**
2. Add a task (notebook, Python script, SQL query, pipeline)
3. Set a schedule (cron expression or UI picker)
4. Choose compute (Serverless or a specific cluster)
5. Save & enable

### Common Schedule Patterns
| Schedule | Cron Expression |
|---|---|
| Every day at 9 AM | `0 9 * * *` |
| Every Monday at 8 AM | `0 8 * * 1` |
| Every hour | `0 * * * *` |
| Every 15 minutes | `*/15 * * * *` |

### Job Parameters
- Pass parameters to notebooks using widgets or job parameters
- Example in notebook: `dbutils.widgets.get("param_name")`

---

## 11. Dashboards

### Creating a Dashboard
1. Run a SQL query in a notebook or SQL editor
2. Click **+** → **Dashboard** to pin results
3. Arrange widgets on the dashboard canvas
4. Add filters, text boxes, and charts
5. Click **Publish** to share

### AI/BI Dashboards (Lakeview)
- Newer dashboard type with rich visualizations
- Go to **Dashboards** → **New Dashboard**
- Drag and drop widgets, configure data sources
- Auto-refresh and sharing built in

---

## 12. Tips & Shortcuts

### Productivity Tips
- **`display(df)` > `df.show()`** — always use `display()` in Databricks for rich tables and charts
- **Mix languages** — use `%sql` for quick queries, `%python` for complex logic, all in one notebook
- **Use `--name` in SQL** — name your SQL results to reuse them in Python cells
- **Cache when needed** — `df.cache()` speeds up repeated operations (don't overuse)
- **Use widgets** — `dbutils.widgets.text("param", "default")` for parameterized notebooks
- **Check schema first** — always run `df.printSchema()` after reading data

### Keyboard Shortcuts (Command Mode — press Esc first)
| Key | Action |
|---|---|
| `Shift+Enter` | Run cell, move to next |
| `Ctrl+Enter` | Run cell, stay |
| `A` | Insert cell above |
| `B` | Insert cell below |
| `D,D` | Delete cell |
| `Z` | Undo delete |
| `M` | Change to Markdown |
| `Y` | Change to code |
| `Cmd+/` (Mac) / `Ctrl+/` (Win) | Toggle comment |
| `Cmd+S` (Mac) / `Ctrl+S` (Win) | Save notebook |

### dbutils Quick Reference
```python
# File system
dbutils.fs.ls("/path/")              # List files
dbutils.fs.cp("/src", "/dst", recurse=True)  # Copy
dbutils.fs.mv("/src", "/dst")      # Move
dbutils.fs.rm("/path", recurse=True)  # Delete

# Widgets (parameters)
dbutils.widgets.text("name", "default_value")
dbutils.widgets.get("name")
dbutils.widgets.dropdown("choice", "A", ["A", "B", "C"])

# Secrets (credentials)
dbutils.secrets.get("scope", "key")

# Notebook utilities
dbutils.notebook.exit("done")       # Exit a notebook job with a value
```

### Getting Help
- Type **?** after any function: `spark.read.csv?`
- Use the built-in assistant (chat) for help with code, debugging, and platform questions
- Press **Cmd+?** (Mac) / **Ctrl+?** (Win) to open the assistant sidebar

---

## Quick Reference: Most Common Code Snippets

```python
# Load data
df = spark.read.csv("/path/to/file.csv", header=True, inferSchema=True)
df = spark.read.table("catalog.schema.table")
df = spark.read.parquet("/path/to/file.parquet")

# Explore
df.display()
df.printSchema()
df.count()

# Transform
df = df.filter(col("status") == "Shipped")
df = df.withColumn("total", col("price") * col("qty"))
df = df.groupBy("cat").agg(sum("total").alias("revenue"))
df = df.orderBy(col("revenue").desc()).limit(10)

# Save
df.write.mode("overwrite").saveAsTable("catalog.schema.my_table")

# SQL in Python
df.createOrReplaceTempView("temp_table")
result = spark.sql("SELECT * FROM temp_table WHERE revenue > 1000")
result.display()
```

---

*This guide covers the essentials. Databricks also supports ML (MLflow), streaming (Structured Streaming), pipelines (Lakeflow), and more — explore the sidebar menus as you grow.*