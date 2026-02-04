# Phase 1: Ingestion and Table Creation

> **Objective:** Create your first Iceberg table and load initial product data  
> **Time Required:** 1-2 hours  
> **Prerequisites:** Databricks environment set up ([02-databricks-setup.md](02-databricks-setup.md))

---

## What You'll Learn

By the end of this phase, you will:

- ✅ Design a schema for SCD Type 2 tracking
- ✅ Choose an appropriate partition strategy
- ✅ Create an Iceberg table in Databricks
- ✅ Load sample product data
- ✅ Inspect the Iceberg metadata files

---

## Step 1: Understand the Business Scenario

### The Context

You're building a data pipeline for an e-commerce company. The product catalog:

- Contains ~10,000 products across multiple categories
- Changes daily (prices update, descriptions change, products discontinued)
- Must maintain full history for auditing and analytics
- Needs to support queries like "What was the price on January 15th?"

### SCD Type 2 Pattern

**Slowly Changing Dimension Type 2** maintains full history by creating new records for each change:

```
┌─────────────────────────────────────────────────────────────────────┐
│  PRODUCT HISTORY TABLE (SCD Type 2)                                  │
├───────────┬──────────┬───────┬────────────┬────────────┬───────────┤
│product_id │ name     │ price │ valid_from │ valid_to   │ is_current│
├───────────┼──────────┼───────┼────────────┼────────────┼───────────┤
│ 1001      │ Widget A │ 19.99 │ 2024-01-01 │ 2024-01-15 │ false     │
│ 1001      │ Widget A │ 24.99 │ 2024-01-15 │ NULL       │ true      │ ← Current
│ 1002      │ Gadget B │ 49.99 │ 2024-01-01 │ NULL       │ true      │ ← Never changed
│ 1003      │ Thing C  │ 9.99  │ 2024-01-01 │ 2024-01-10 │ false     │ ← Discontinued
└───────────┴──────────┴───────┴────────────┴────────────┴───────────┘
```

**Key Columns:**
- `valid_from`: When this version of the record became active
- `valid_to`: When this version was superseded (NULL = current)
- `is_current`: Quick filter for current records (redundant but useful)

---

## Step 2: Design Your Schema

### Full Schema Definition

```
┌─────────────────────────────────────────────────────────────────────┐
│  COLUMN NAME         │ DATA TYPE      │ PURPOSE                     │
├──────────────────────┼────────────────┼─────────────────────────────┤
│ product_id           │ BIGINT         │ Business key (not unique)   │
│ product_name         │ STRING         │ Display name                │
│ category             │ STRING         │ Product category            │
│ subcategory          │ STRING         │ More specific grouping      │
│ brand                │ STRING         │ Brand name                  │
│ price                │ DECIMAL(10,2)  │ Current price               │
│ cost                 │ DECIMAL(10,2)  │ Cost to company             │
│ description          │ STRING         │ Product description         │
│ is_active            │ BOOLEAN        │ Still sold? (not discontinued)│
│ ─────────────────────┼────────────────┼───────────────────────────────│
│ valid_from           │ TIMESTAMP      │ SCD: When version started   │
│ valid_to             │ TIMESTAMP      │ SCD: When version ended     │
│ is_current           │ BOOLEAN        │ SCD: Is this latest version?│
│ ─────────────────────┼────────────────┼───────────────────────────────│
│ ingestion_timestamp  │ TIMESTAMP      │ Lineage: When we loaded it  │
│ source_file          │ STRING         │ Lineage: Source file name   │
│ record_hash          │ STRING         │ Change detection hash       │
└──────────────────────┴────────────────┴─────────────────────────────┘
```

### Why These Columns?

| Column Group | Purpose |
|--------------|---------|
| Business columns | The actual product data |
| SCD columns | Track historical changes |
| Lineage columns | Debugging and auditing |

---

## Step 3: Choose Partition Strategy

### Considerations

| Factor | Our Situation |
|--------|--------------|
| **Query patterns** | Most queries filter by category or date range |
| **Data volume** | ~10,000 products × 365 days = ~3.6M records/year |
| **Update frequency** | Daily batch loads |
| **File size target** | 128MB - 1GB per file |

### Recommended Strategy

```
PARTITIONED BY (
    bucket(16, product_id),    -- Distribute products evenly
    month(valid_from)           -- Time-based partitioning
)
```

**Why this combination?**

1. **bucket(16, product_id)**: 
   - Spreads data across 16 buckets
   - Prevents hot spots from popular products
   - Good for point lookups by product_id

2. **month(valid_from)**:
   - Aligns with business reporting (monthly)
   - Enables efficient date range queries
   - Not too granular (avoid small files)

### Alternative Strategies

| Strategy | Best When |
|----------|-----------|
| `day(valid_from)` | High volume, daily queries common |
| `identity(category)` | Few categories, category-first queries |
| `bucket(N, product_id)` only | No date-based queries |

---

## Step 4: Create Database Structure

### Step 4.1: Open Your Notebook

1. In Databricks, navigate to your workspace
2. Open or create notebook: `notebooks/01-setup-and-initial-load`
3. Attach to your cluster

### Step 4.2: Create the Database

```sql
-- Cell 1: Create database structure
-- ================================

-- Create a database for our project
CREATE DATABASE IF NOT EXISTS iceberg_learning
COMMENT 'Learning project for Iceberg on Databricks';

-- Use the database
USE iceberg_learning;

-- Verify
SELECT current_database();
```

**Expected Output:**
```
iceberg_learning
```

---

## Step 5: Create the Iceberg Table

### Step 5.1: Create Table with Full Schema

```sql
-- Cell 2: Create the product_history table
-- ========================================

CREATE TABLE IF NOT EXISTS product_history (
    -- Business columns
    product_id           BIGINT          COMMENT 'Unique product identifier',
    product_name         STRING          COMMENT 'Product display name',
    category             STRING          COMMENT 'Product category',
    subcategory          STRING          COMMENT 'Product subcategory',
    brand                STRING          COMMENT 'Brand name',
    price                DECIMAL(10,2)   COMMENT 'Selling price',
    cost                 DECIMAL(10,2)   COMMENT 'Cost to company',
    description          STRING          COMMENT 'Product description',
    is_active            BOOLEAN         COMMENT 'Product is actively sold',
    
    -- SCD Type 2 columns
    valid_from           TIMESTAMP       COMMENT 'When this version became active',
    valid_to             TIMESTAMP       COMMENT 'When this version was superseded',
    is_current           BOOLEAN         COMMENT 'Is this the current version',
    
    -- Lineage columns
    ingestion_timestamp  TIMESTAMP       COMMENT 'When record was loaded',
    source_file          STRING          COMMENT 'Source file name',
    record_hash          STRING          COMMENT 'Hash for change detection'
)
USING ICEBERG
PARTITIONED BY (bucket(16, product_id), month(valid_from))
TBLPROPERTIES (
    'write.format.default' = 'parquet',
    'write.parquet.compression-codec' = 'snappy',
    'format-version' = '2'
)
COMMENT 'Product catalog with full SCD Type 2 history';
```

### Step 5.2: Verify Table Creation

```sql
-- Cell 3: Verify table was created
-- =================================

-- Check table exists
SHOW TABLES IN iceberg_learning;

-- View table details
DESCRIBE EXTENDED product_history;
```

**Expected Output:**
```
┌──────────────────────┬─────────────────────────────────────┐
│ col_name             │ data_type                           │
├──────────────────────┼─────────────────────────────────────┤
│ product_id           │ bigint                              │
│ product_name         │ string                              │
│ ...                  │ ...                                 │
│                      │                                     │
│ # Partitioning       │                                     │
│ Part 0               │ bucket(16, product_id)              │
│ Part 1               │ month(valid_from)                   │
│                      │                                     │
│ # Table Properties   │                                     │
│ Type                 │ ICEBERG                             │
│ Provider             │ iceberg                             │
└──────────────────────┴─────────────────────────────────────┘
```

---

## Step 6: Generate Sample Data

### Step 6.1: Create Sample Data Generator

```python
# Cell 4: Generate sample product data
# ====================================

from pyspark.sql.functions import (
    lit, current_timestamp, md5, concat_ws, col,
    rand, round as spark_round, when
)
from pyspark.sql.types import *
from datetime import datetime, timedelta
import random

# Define categories and their subcategories
categories = {
    "Electronics": ["Laptops", "Phones", "Tablets", "Accessories", "Cameras"],
    "Clothing": ["Men", "Women", "Kids", "Sports", "Accessories"],
    "Home": ["Furniture", "Kitchen", "Decor", "Garden", "Storage"],
    "Sports": ["Fitness", "Outdoor", "Team Sports", "Water Sports", "Winter"],
    "Books": ["Fiction", "Non-Fiction", "Technical", "Children", "Educational"]
}

# Define brands per category
brands = {
    "Electronics": ["TechCorp", "GadgetPro", "DigiMax", "SmartLife", "ElectraWave"],
    "Clothing": ["FashionFirst", "StyleHub", "TrendSetters", "ComfortWear", "ActiveLife"],
    "Home": ["HomeEssentials", "LivingSpace", "CozyNest", "ModernHome", "NatureLiving"],
    "Sports": ["AthletePro", "FitGear", "SportsMaster", "ActiveZone", "ChampionKit"],
    "Books": ["PageTurner", "KnowledgePress", "StoryTime", "LearnMore", "BookWorm"]
}

# Generate product records
products = []
product_id = 1000

for category, subcats in categories.items():
    for subcategory in subcats:
        # Generate 40 products per subcategory (5 categories × 5 subcats × 40 = 1000 products)
        for i in range(40):
            brand = random.choice(brands[category])
            base_price = random.uniform(9.99, 299.99)
            
            products.append({
                "product_id": product_id,
                "product_name": f"{brand} {subcategory} Item {i+1}",
                "category": category,
                "subcategory": subcategory,
                "brand": brand,
                "price": round(base_price, 2),
                "cost": round(base_price * random.uniform(0.4, 0.7), 2),
                "description": f"High-quality {subcategory.lower()} product from {brand}",
                "is_active": True
            })
            product_id += 1

# Create DataFrame
schema = StructType([
    StructField("product_id", LongType(), False),
    StructField("product_name", StringType(), False),
    StructField("category", StringType(), False),
    StructField("subcategory", StringType(), False),
    StructField("brand", StringType(), False),
    StructField("price", DoubleType(), False),
    StructField("cost", DoubleType(), False),
    StructField("description", StringType(), False),
    StructField("is_active", BooleanType(), False)
])

df_products = spark.createDataFrame(products, schema)

# Add SCD and lineage columns
initial_load_date = datetime(2024, 1, 1, 0, 0, 0)

df_with_scd = df_products \
    .withColumn("valid_from", lit(initial_load_date)) \
    .withColumn("valid_to", lit(None).cast(TimestampType())) \
    .withColumn("is_current", lit(True)) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("initial_load_2024_01_01.csv")) \
    .withColumn("record_hash", md5(concat_ws("||", 
        col("product_id"), col("product_name"), col("price"), 
        col("description"), col("is_active")
    )))

# Cast price and cost to DECIMAL
df_final = df_with_scd \
    .withColumn("price", col("price").cast(DecimalType(10, 2))) \
    .withColumn("cost", col("cost").cast(DecimalType(10, 2)))

print(f"Generated {df_final.count()} products")
df_final.show(5, truncate=False)
```

**Expected Output:**
```
Generated 1000 products
+----------+---------------------------+-----------+-----------+--------+------+-----+...
|product_id|product_name               |category   |subcategory|brand   |price |cost |...
+----------+---------------------------+-----------+-----------+--------+------+-----+...
|1000      |TechCorp Laptops Item 1    |Electronics|Laptops    |TechCorp|149.99|89.99|...
|1001      |GadgetPro Laptops Item 2   |Electronics|Laptops    |GadgetPro|79.99|47.99|...
...
```

---

## Step 7: Load Data into Iceberg Table

### Step 7.1: Write Data

```python
# Cell 5: Write data to Iceberg table
# ===================================

# Write the DataFrame to our Iceberg table
df_final.writeTo("iceberg_learning.product_history").append()

print("Data written successfully!")
```

### Step 7.2: Verify Data Loaded

```sql
-- Cell 6: Verify data loaded correctly
-- ====================================

-- Count records
SELECT COUNT(*) as total_records FROM product_history;

-- Check category distribution
SELECT 
    category, 
    COUNT(*) as product_count,
    ROUND(AVG(price), 2) as avg_price
FROM product_history
GROUP BY category
ORDER BY product_count DESC;

-- Sample records
SELECT * FROM product_history LIMIT 5;
```

**Expected Output:**
```
total_records: 1000

category     | product_count | avg_price
-------------|---------------|----------
Electronics  | 200           | 154.32
Clothing     | 200           | 89.45
Home         | 200           | 123.67
Sports       | 200           | 98.23
Books        | 200           | 45.78
```

---

## Step 8: Inspect Iceberg Metadata

### Step 8.1: View Snapshots

```sql
-- Cell 7: Explore Iceberg metadata
-- ================================

-- View snapshots (should have 1 after initial load)
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary
FROM iceberg_learning.product_history.snapshots;
```

**Expected Output:**
```
snapshot_id      | committed_at              | operation | summary
-----------------|---------------------------|-----------|---------------------------
12345678901234   | 2024-01-15 10:30:00.000  | append    | {added-records -> 1000, ...}
```

### Step 8.2: View History

```sql
-- View operation history
SELECT 
    made_current_at,
    snapshot_id,
    is_current_ancestor
FROM iceberg_learning.product_history.history
ORDER BY made_current_at;
```

### Step 8.3: View Data Files

```sql
-- View data files created
SELECT 
    file_path,
    file_format,
    record_count,
    file_size_in_bytes,
    partition
FROM iceberg_learning.product_history.files;
```

**What to observe:**
- Multiple files created (due to bucketing)
- Each file shows its partition values
- Record counts distributed across files

### Step 8.4: View Manifests

```sql
-- View manifest files
SELECT 
    path,
    length,
    partition_spec_id,
    added_files_count,
    added_rows_count
FROM iceberg_learning.product_history.manifests;
```

---

## Step 9: Explore Storage (Optional but Recommended)

### For Community Edition (DBFS)

```python
# Cell 8: Explore the physical storage
# ====================================

# Find the table location
table_location = spark.sql("""
    DESCRIBE EXTENDED product_history
""").filter("col_name = 'Location'").collect()[0][1]

print(f"Table location: {table_location}")

# List metadata files
print("\n=== Metadata Files ===")
metadata_path = f"{table_location}/metadata"
try:
    files = dbutils.fs.ls(metadata_path)
    for f in files[:10]:  # Show first 10
        print(f"  {f.name} ({f.size} bytes)")
except:
    print("  Unable to list metadata (this is OK in some configurations)")

# List data files
print("\n=== Data Files (sample) ===")
data_path = f"{table_location}/data"
try:
    files = dbutils.fs.ls(data_path)
    for f in files[:5]:  # Show first 5
        print(f"  {f.name}")
except:
    print("  Unable to list data (this is OK in some configurations)")
```

**Expected Output:**
```
Table location: dbfs:/user/hive/warehouse/iceberg_learning.db/product_history

=== Metadata Files ===
  v1.metadata.json (4532 bytes)
  snap-12345678901234-1-abc.avro (1234 bytes)
  abc-m0.avro (5678 bytes)

=== Data Files (sample) ===
  product_id_bucket=0/
  product_id_bucket=1/
  product_id_bucket=2/
  ...
```

---

## Validation Checklist

Before moving to Phase 2, verify:

- [ ] Database `iceberg_learning` exists
- [ ] Table `product_history` created with correct schema
- [ ] Partition spec shows `bucket(16, product_id)` and `month(valid_from)`
- [ ] 1,000 records loaded successfully
- [ ] Snapshot created (visible in `.snapshots` table)
- [ ] Data files distributed across bucket partitions
- [ ] Can query the data successfully

---

## Common Issues and Solutions

### "Table already exists"

```sql
-- Drop and recreate if needed
DROP TABLE IF EXISTS product_history;
-- Then run CREATE TABLE again
```

### "Cannot resolve column"

```python
# Check DataFrame schema matches table schema
df_final.printSchema()

# Compare with table schema
spark.sql("DESCRIBE product_history").show()
```

### "Partition column not found"

Ensure partition columns exist in your DataFrame before writing:
```python
# Partition columns must be present
print("Columns:", df_final.columns)
assert "product_id" in df_final.columns
assert "valid_from" in df_final.columns
```

---

## Key Learnings from Phase 1

1. **Schema Design Matters** — Plan for SCD columns upfront; adding them later is possible but easier to start with them.

2. **Partition Strategy Affects Everything** — Choose based on query patterns; can evolve later but best to get it right initially.

3. **Iceberg Tracks Everything** — Every write creates a snapshot; metadata tables let you see exactly what happened.

4. **Files Are Distributed** — Bucketing spreads data across multiple files for parallel processing.

---

## Next Steps

Proceed to **[Phase 2: Merge Operations](04-phase2-merge-operations.md)** where you'll:

- Generate "Day 2" product changes
- Implement MERGE INTO for upserts
- See how Iceberg handles updates
- Build up snapshot history
