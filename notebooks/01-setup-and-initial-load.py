# Databricks notebook source

# MAGIC %md
# MAGIC # Phase 1: Setup and Initial Data Load
# MAGIC
# MAGIC **Objective:** Create the Iceberg-backed `product_history` table and load 1,000 sample products.
# MAGIC
# MAGIC **What you'll learn:**
# MAGIC - Verify your Databricks + Iceberg environment
# MAGIC - Design a schema for SCD Type 2 tracking
# MAGIC - Create an Iceberg table with bucket + month partitioning
# MAGIC - Generate and load sample product data
# MAGIC - Inspect Iceberg metadata (snapshots, history, files, manifests)
# MAGIC
# MAGIC **Prerequisites:** Databricks cluster running DBR 13.3 LTS+ (see `docs/02-databricks-setup.md`)
# MAGIC
# MAGIC ---

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Environment Verification
# MAGIC Confirm Spark version, Databricks runtime, and basic DBFS access.

# COMMAND ----------

# Cell 1: Verify environment
# ===========================

import os

# Spark & runtime versions
print(f"Spark version : {spark.version}")
print(f"Databricks RT : {os.environ.get('DATABRICKS_RUNTIME_VERSION', 'Unknown')}")

# Quick sanity check – list DBFS root (should not error)
display(dbutils.fs.ls("/"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Create Database
# MAGIC Set up the `iceberg_learning` database that will hold all tables for this project.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 2: Create the project database
# MAGIC -- ====================================
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS iceberg_learning
# MAGIC COMMENT 'Learning project for Apache Iceberg on Databricks';
# MAGIC
# MAGIC USE iceberg_learning;
# MAGIC
# MAGIC -- Confirm we're in the right database
# MAGIC SELECT current_database() AS current_db;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Create the Iceberg Table
# MAGIC
# MAGIC The `product_history` table uses an **SCD Type 2** design:
# MAGIC
# MAGIC | Column Group | Purpose |
# MAGIC |---|---|
# MAGIC | Business columns | Actual product attributes |
# MAGIC | SCD columns (`valid_from`, `valid_to`, `is_current`) | Track historical changes |
# MAGIC | Lineage columns (`ingestion_timestamp`, `source_file`, `record_hash`) | Debugging & auditing |
# MAGIC
# MAGIC **Partition strategy:**
# MAGIC - `bucket(16, product_id)` — distributes products evenly, good for point lookups
# MAGIC - `month(valid_from)` — aligns with monthly reporting, efficient date-range queries

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 3: Create the product_history Iceberg table
# MAGIC -- ==================================================
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS product_history (
# MAGIC     -- Business columns
# MAGIC     product_id           BIGINT          COMMENT 'Unique product identifier',
# MAGIC     product_name         STRING          COMMENT 'Product display name',
# MAGIC     category             STRING          COMMENT 'Product category',
# MAGIC     subcategory          STRING          COMMENT 'Product subcategory',
# MAGIC     brand                STRING          COMMENT 'Brand name',
# MAGIC     price                DECIMAL(10,2)   COMMENT 'Selling price',
# MAGIC     cost                 DECIMAL(10,2)   COMMENT 'Cost to company',
# MAGIC     description          STRING          COMMENT 'Product description',
# MAGIC     is_active            BOOLEAN         COMMENT 'Product is actively sold',
# MAGIC
# MAGIC     -- SCD Type 2 columns
# MAGIC     valid_from           TIMESTAMP       COMMENT 'When this version became active',
# MAGIC     valid_to             TIMESTAMP       COMMENT 'When this version was superseded (NULL = current)',
# MAGIC     is_current           BOOLEAN         COMMENT 'Is this the current version',
# MAGIC
# MAGIC     -- Lineage columns
# MAGIC     ingestion_timestamp  TIMESTAMP       COMMENT 'When record was loaded',
# MAGIC     source_file          STRING          COMMENT 'Source file name',
# MAGIC     record_hash          STRING          COMMENT 'Hash for change detection'
# MAGIC )
# MAGIC USING ICEBERG
# MAGIC PARTITIONED BY (bucket(16, product_id), month(valid_from))
# MAGIC TBLPROPERTIES (
# MAGIC     'write.format.default'             = 'parquet',
# MAGIC     'write.parquet.compression-codec'  = 'snappy',
# MAGIC     'format-version'                   = '2'
# MAGIC )
# MAGIC COMMENT 'Product catalog with full SCD Type 2 history';

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Verify Table Creation
# MAGIC Confirm schema, partitioning, and Iceberg table properties.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 4: Verify the table
# MAGIC -- =========================
# MAGIC
# MAGIC -- List tables in the database
# MAGIC SHOW TABLES IN iceberg_learning;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 5: Inspect full table details (schema + partitions + properties)
# MAGIC -- =====================================================================
# MAGIC
# MAGIC DESCRIBE EXTENDED product_history;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Generate Sample Product Data
# MAGIC
# MAGIC Build a DataFrame of **1,000 products** across 5 categories, 25 subcategories, and 25 brands.
# MAGIC Each row already carries the SCD Type 2 and lineage columns required by the table.

# COMMAND ----------

# Cell 6: Generate 1,000 sample products
# =======================================

from pyspark.sql.functions import (
    lit, current_timestamp, md5, concat_ws, col
)
from pyspark.sql.types import (
    StructType, StructField,
    LongType, StringType, DoubleType, BooleanType,
    TimestampType, DecimalType
)
from datetime import datetime
import random

# ------------------------------------------------------------------
# 5a. Reference data
# ------------------------------------------------------------------
categories = {
    "Electronics": ["Laptops", "Phones", "Tablets", "Accessories", "Cameras"],
    "Clothing":    ["Men", "Women", "Kids", "Sports", "Accessories"],
    "Home":        ["Furniture", "Kitchen", "Decor", "Garden", "Storage"],
    "Sports":      ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Winter"],
    "Books":       ["Fiction", "Non-Fiction", "Technical", "Children", "Educational"],
}

brands = {
    "Electronics": ["TechCorp", "GadgetPro", "DigiMax", "SmartLife", "ElectraWave"],
    "Clothing":    ["FashionFirst", "StyleHub", "TrendSetters", "ComfortWear", "ActiveLife"],
    "Home":        ["HomeEssentials", "LivingSpace", "CozyNest", "ModernHome", "NatureLiving"],
    "Sports":      ["AthletePro", "FitGear", "SportsMaster", "ActiveZone", "ChampionKit"],
    "Books":       ["PageTurner", "KnowledgePress", "StoryTime", "LearnMore", "BookWorm"],
}

# ------------------------------------------------------------------
# 5b. Build product rows
# ------------------------------------------------------------------
random.seed(42)  # reproducible data
products = []
product_id = 1000

for category, subcats in categories.items():
    for subcategory in subcats:
        # 40 products per subcategory → 5 × 5 × 40 = 1,000 total
        for i in range(40):
            brand = random.choice(brands[category])
            base_price = random.uniform(9.99, 299.99)

            products.append({
                "product_id":   product_id,
                "product_name": f"{brand} {subcategory} Item {i + 1}",
                "category":     category,
                "subcategory":  subcategory,
                "brand":        brand,
                "price":        round(base_price, 2),
                "cost":         round(base_price * random.uniform(0.4, 0.7), 2),
                "description":  f"High-quality {subcategory.lower()} product from {brand}",
                "is_active":    True,
            })
            product_id += 1

# ------------------------------------------------------------------
# 5c. Create Spark DataFrame
# ------------------------------------------------------------------
schema = StructType([
    StructField("product_id",   LongType(),    False),
    StructField("product_name", StringType(),  False),
    StructField("category",     StringType(),  False),
    StructField("subcategory",  StringType(),  False),
    StructField("brand",        StringType(),  False),
    StructField("price",        DoubleType(),  False),
    StructField("cost",         DoubleType(),  False),
    StructField("description",  StringType(),  False),
    StructField("is_active",    BooleanType(), False),
])

df_products = spark.createDataFrame(products, schema)

# ------------------------------------------------------------------
# 5d. Add SCD Type 2 + lineage columns
# ------------------------------------------------------------------
initial_load_date = datetime(2024, 1, 1, 0, 0, 0)

df_final = (
    df_products
    .withColumn("valid_from",          lit(initial_load_date))
    .withColumn("valid_to",            lit(None).cast(TimestampType()))
    .withColumn("is_current",          lit(True))
    .withColumn("ingestion_timestamp", current_timestamp())
    .withColumn("source_file",         lit("initial_load_2024_01_01.csv"))
    .withColumn("record_hash",         md5(concat_ws("||",
        col("product_id"), col("product_name"), col("price"),
        col("description"), col("is_active")
    )))
    # Cast price/cost to match table's DECIMAL(10,2)
    .withColumn("price", col("price").cast(DecimalType(10, 2)))
    .withColumn("cost",  col("cost").cast(DecimalType(10, 2)))
)

print(f"Generated {df_final.count()} products")
df_final.printSchema()
df_final.show(5, truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Load Data into the Iceberg Table
# MAGIC Append the generated DataFrame into `product_history`.

# COMMAND ----------

# Cell 7: Write data to the Iceberg table
# ========================================

df_final.writeTo("iceberg_learning.product_history").append()

print("✅ Data written successfully to iceberg_learning.product_history")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Verify the Load
# MAGIC Run a few quick queries to make sure data landed correctly.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 8: Record count
# MAGIC -- ====================
# MAGIC
# MAGIC SELECT COUNT(*) AS total_records FROM product_history;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 9: Category distribution
# MAGIC -- ==============================
# MAGIC
# MAGIC SELECT
# MAGIC     category,
# MAGIC     COUNT(*)         AS product_count,
# MAGIC     ROUND(AVG(price), 2) AS avg_price,
# MAGIC     ROUND(MIN(price), 2) AS min_price,
# MAGIC     ROUND(MAX(price), 2) AS max_price
# MAGIC FROM product_history
# MAGIC GROUP BY category
# MAGIC ORDER BY product_count DESC;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 10: Sample rows
# MAGIC -- ====================
# MAGIC
# MAGIC SELECT * FROM product_history LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Inspect Iceberg Metadata
# MAGIC
# MAGIC Every write creates a **snapshot** — an immutable point-in-time view of the table.
# MAGIC Iceberg exposes this metadata through special dot-notation tables.

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 11: Snapshots (should show 1 after the initial load)
# MAGIC -- ==========================================================
# MAGIC
# MAGIC SELECT
# MAGIC     snapshot_id,
# MAGIC     committed_at,
# MAGIC     operation,
# MAGIC     summary
# MAGIC FROM iceberg_learning.product_history.snapshots;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 12: History
# MAGIC -- ================
# MAGIC
# MAGIC SELECT
# MAGIC     made_current_at,
# MAGIC     snapshot_id,
# MAGIC     is_current_ancestor
# MAGIC FROM iceberg_learning.product_history.history
# MAGIC ORDER BY made_current_at;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 13: Data files — see how bucketing distributed the data
# MAGIC -- ==============================================================
# MAGIC
# MAGIC SELECT
# MAGIC     file_path,
# MAGIC     file_format,
# MAGIC     record_count,
# MAGIC     file_size_in_bytes,
# MAGIC     partition
# MAGIC FROM iceberg_learning.product_history.files;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Cell 14: Manifests
# MAGIC -- ==================
# MAGIC
# MAGIC SELECT
# MAGIC     path,
# MAGIC     length,
# MAGIC     partition_spec_id,
# MAGIC     added_files_count,
# MAGIC     added_rows_count
# MAGIC FROM iceberg_learning.product_history.manifests;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Explore Physical Storage (optional)
# MAGIC Peek at the actual metadata and data files on disk/DBFS.

# COMMAND ----------

# Cell 15: Explore the physical storage layout
# =============================================

# Get the table's storage location from DESCRIBE EXTENDED
table_location = (
    spark.sql("DESCRIBE EXTENDED product_history")
    .filter("col_name = 'Location'")
    .collect()[0][1]
)
print(f"Table location: {table_location}\n")

# --- Metadata files ---
print("=== Metadata Files ===")
metadata_path = f"{table_location}/metadata"
try:
    for f in dbutils.fs.ls(metadata_path)[:10]:
        print(f"  {f.name}  ({f.size:,} bytes)")
except Exception as e:
    print(f"  Unable to list metadata: {e}")

# --- Data files (top-level partitions) ---
print("\n=== Data Directories (sample) ===")
data_path = f"{table_location}/data"
try:
    for f in dbutils.fs.ls(data_path)[:8]:
        print(f"  {f.name}")
except Exception as e:
    print(f"  Unable to list data: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 10. Validation Checklist
# MAGIC
# MAGIC Run the cell below to programmatically verify every Phase 1 requirement.

# COMMAND ----------

# Cell 16: Automated validation
# ==============================

checks_passed = 0
checks_total  = 0

def check(description, condition):
    global checks_passed, checks_total
    checks_total += 1
    status = "PASS" if condition else "FAIL"
    if condition:
        checks_passed += 1
    print(f"  [{status}] {description}")

print("Phase 1 — Validation Checklist")
print("=" * 50)

# 1. Database exists
dbs = [row.databaseName for row in spark.sql("SHOW DATABASES").collect()]
check("Database 'iceberg_learning' exists", "iceberg_learning" in dbs)

# 2. Table exists
tables = [row.tableName for row in spark.sql("SHOW TABLES IN iceberg_learning").collect()]
check("Table 'product_history' exists", "product_history" in tables)

# 3. Correct record count
count = spark.sql("SELECT COUNT(*) AS cnt FROM iceberg_learning.product_history").collect()[0]["cnt"]
check(f"Record count is 1,000 (actual: {count})", count == 1000)

# 4. Partition spec includes bucket and month
desc_rows = spark.sql("DESCRIBE EXTENDED iceberg_learning.product_history").collect()
desc_text = " ".join([str(r) for r in desc_rows])
check("Partition spec contains 'bucket(16, product_id)'",  "bucket(16, product_id)"  in desc_text)
check("Partition spec contains 'month(valid_from)'",       "month(valid_from)"       in desc_text)

# 5. At least 1 snapshot
snap_count = spark.sql(
    "SELECT COUNT(*) AS cnt FROM iceberg_learning.product_history.snapshots"
).collect()[0]["cnt"]
check(f"At least 1 snapshot exists (actual: {snap_count})", snap_count >= 1)

# 6. All is_current flags are True (initial load)
non_current = spark.sql("""
    SELECT COUNT(*) AS cnt
    FROM iceberg_learning.product_history
    WHERE is_current = false
""").collect()[0]["cnt"]
check(f"All records have is_current = true (non-current: {non_current})", non_current == 0)

# 7. All valid_to values are NULL (initial load)
closed = spark.sql("""
    SELECT COUNT(*) AS cnt
    FROM iceberg_learning.product_history
    WHERE valid_to IS NOT NULL
""").collect()[0]["cnt"]
check(f"All valid_to values are NULL (closed: {closed})", closed == 0)

print("=" * 50)
print(f"Result: {checks_passed}/{checks_total} checks passed")
if checks_passed == checks_total:
    print("\n🎉 Phase 1 complete — you're ready for Phase 2 (Merge Operations)!")
else:
    print("\n⚠️  Some checks failed. Review the output above before continuing.")

# COMMAND ----------

# MAGIC %md
# MAGIC ---
# MAGIC ## Next Steps
# MAGIC
# MAGIC Proceed to **`02-merge-operations.py`** (Phase 2) where you will:
# MAGIC - Generate "Day 2" product changes (price updates, new products, discontinuations)
# MAGIC - Implement `MERGE INTO` for SCD Type 2 upserts
# MAGIC - Observe how Iceberg handles updates (Copy-on-Write vs Merge-on-Read)
# MAGIC - Build up snapshot history
