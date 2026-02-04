# Phase 4: Schema and Partition Evolution

> **Objective:** Evolve your table without rewriting existing data  
> **Time Required:** 1 hour  
> **Prerequisites:** Completed [Phase 3](05-phase3-time-travel.md)

---

## What You'll Learn

By the end of this phase, you will:

- ✅ Add columns without rewriting data
- ✅ Rename and drop columns safely
- ✅ Change partition strategy on a live table
- ✅ Understand how Iceberg tracks schema changes
- ✅ See old and new data coexist seamlessly

---

## Step 1: Understand Schema Evolution

### The Traditional Problem

In traditional data lakes:

```
BEFORE: Schema v1
┌──────────┬──────────┬──────────┐
│ id       │ name     │ price    │
└──────────┴──────────┴──────────┘
           ↓ Want to add "rating" column
           
TRADITIONAL APPROACH:
1. Create new table with new schema
2. Copy ALL data from old table
3. Drop old table
4. Rename new table

Time: Hours to days
Risk: High (data loss possible)
Downtime: Required
```

### The Iceberg Way

```
ICEBERG APPROACH:
┌──────────────────────────────────────────────────────────────┐
│  ALTER TABLE products ADD COLUMN rating DOUBLE               │
└──────────────────────────────────────────────────────────────┘
           ↓
┌──────────────────────────────────────────────────────────────┐
│  Only metadata changes!                                      │
│                                                              │
│  metadata.json updated:                                      │
│    schemas: [                                                │
│      { schema-id: 0, fields: [id, name, price] },           │
│      { schema-id: 1, fields: [id, name, price, rating] }    │ ← NEW
│    ]                                                         │
│    current-schema-id: 1  ← Points to new schema              │
│                                                              │
│  Old data files: UNCHANGED (still have old schema)           │
│  Queries: Return NULL for rating on old files               │
└──────────────────────────────────────────────────────────────┘

Time: Seconds
Risk: Zero
Downtime: None
```

### Why This Works: Column IDs

Iceberg tracks columns by **unique numeric ID**, not by name or position:

```
Schema Version 0:              Schema Version 1:
┌────┬────────┬────────┐      ┌────┬────────┬────────┐
│ ID │ Name   │ Type   │      │ ID │ Name   │ Type   │
├────┼────────┼────────┤      ├────┼────────┼────────┤
│ 1  │ id     │ long   │      │ 1  │ id     │ long   │
│ 2  │ name   │ string │      │ 2  │ name   │ string │
│ 3  │ price  │ double │      │ 3  │ price  │ double │
└────┴────────┴────────┘      │ 4  │ rating │ double │ ← New ID
                              └────┴────────┴────────┘

Old data files:
  Have columns with IDs [1, 2, 3]
  
New data files:
  Have columns with IDs [1, 2, 3, 4]
  
Query for "rating" (ID 4):
  Old files → Column ID 4 not found → Return NULL
  New files → Column ID 4 exists → Return value
```

---

## Step 2: Add New Columns

### Step 2.1: The Business Requirement

> "The sustainability team wants to track eco-ratings for products.
> We need to add a `sustainability_rating` column (1-5 scale)
> and a `eco_certified` boolean flag."

### Step 2.2: Check Current Schema

```sql
-- Cell 1: View current schema
-- ===========================

DESCRIBE iceberg_learning.product_history;
```

### Step 2.3: Add the New Columns

```sql
-- Cell 2: Add sustainability columns
-- ==================================

-- Add sustainability_rating (nullable by default)
ALTER TABLE iceberg_learning.product_history
ADD COLUMN sustainability_rating INT 
COMMENT 'Eco-friendliness rating 1-5';

-- Add eco_certified flag
ALTER TABLE iceberg_learning.product_history
ADD COLUMN eco_certified BOOLEAN 
COMMENT 'Product has eco certification';

-- Verify changes
DESCRIBE iceberg_learning.product_history;
```

**Expected Output:**
```
col_name              | data_type | comment
----------------------|-----------|----------------------------------
product_id            | bigint    | Unique product identifier
...
sustainability_rating | int       | Eco-friendliness rating 1-5
eco_certified         | boolean   | Product has eco certification
```

### Step 2.4: Verify Old Data Still Works

```sql
-- Cell 3: Query old data with new schema
-- ======================================

-- Old records return NULL for new columns
SELECT 
    product_id,
    product_name,
    price,
    sustainability_rating,  -- Will be NULL
    eco_certified,          -- Will be NULL
    valid_from
FROM iceberg_learning.product_history
WHERE valid_from = '2024-01-01'
LIMIT 5;
```

**Expected Output:**
```
product_id | product_name | price | sustainability_rating | eco_certified | valid_from
-----------|--------------|-------|----------------------|---------------|------------
1000       | Widget A     | 19.99 | NULL                 | NULL          | 2024-01-01
1001       | Gadget B     | 29.99 | NULL                 | NULL          | 2024-01-01
...
```

### Step 2.5: Insert Data with New Columns

```python
# Cell 4: Add data with new columns
# =================================

from datetime import datetime
from pyspark.sql.functions import *

# Create new product data with sustainability info
new_products = [
    (2001, "Eco Widget Pro", "Electronics", "Laptops", "GreenTech", 
     299.99, 150.00, "Sustainable laptop", True,
     datetime(2024, 2, 1), None, True,
     datetime.now(), "eco_products_2024_02_01.csv", "hash123",
     4, True),  # Has sustainability data
    
    (2002, "Solar Charger", "Electronics", "Accessories", "SunPower",
     49.99, 25.00, "Solar-powered charger", True,
     datetime(2024, 2, 1), None, True,
     datetime.now(), "eco_products_2024_02_01.csv", "hash124",
     5, True),  # Has sustainability data
]

columns = [
    "product_id", "product_name", "category", "subcategory", "brand",
    "price", "cost", "description", "is_active",
    "valid_from", "valid_to", "is_current",
    "ingestion_timestamp", "source_file", "record_hash",
    "sustainability_rating", "eco_certified"
]

df_new = spark.createDataFrame(new_products, columns)
df_new = df_new.withColumn("price", col("price").cast("decimal(10,2)")) \
               .withColumn("cost", col("cost").cast("decimal(10,2)"))

# Write to table
df_new.writeTo("iceberg_learning.product_history").append()

print("New products with sustainability data added!")
```

### Step 2.6: Query Mixed Data

```sql
-- Cell 5: Query showing old and new data together
-- ===============================================

SELECT 
    product_id,
    product_name,
    sustainability_rating,
    eco_certified,
    valid_from,
    CASE 
        WHEN sustainability_rating IS NULL THEN 'Pre-sustainability era'
        ELSE 'Has rating'
    END as data_era
FROM iceberg_learning.product_history
WHERE is_current = true
ORDER BY sustainability_rating DESC NULLS LAST
LIMIT 10;
```

---

## Step 3: Rename and Reorder Columns

### Step 3.1: Rename a Column

```sql
-- Cell 6: Rename column
-- =====================

-- Rename 'product_name' to 'name' 
-- (demonstrating the capability - we'll rename it back)

-- First, let's rename a less critical column for demonstration
ALTER TABLE iceberg_learning.product_history
RENAME COLUMN subcategory TO product_subcategory;

-- Verify
DESCRIBE iceberg_learning.product_history;

-- Rename it back
ALTER TABLE iceberg_learning.product_history
RENAME COLUMN product_subcategory TO subcategory;
```

### Step 3.2: Reorder Columns (Display Order)

```sql
-- Cell 7: Reorder columns
-- =======================

-- Move sustainability_rating after price
ALTER TABLE iceberg_learning.product_history
ALTER COLUMN sustainability_rating AFTER price;

-- Verify new order
DESCRIBE iceberg_learning.product_history;
```

**Note:** Reordering only affects how columns appear in `DESCRIBE` and `SELECT *`. Data files are not rewritten.

---

## Step 4: Drop Columns

### Step 4.1: Understanding Column Drops

```
WHAT HAPPENS WHEN YOU DROP A COLUMN:
────────────────────────────────────

Metadata:
  Column removed from current schema
  Column ID marked as "deleted"
  
Data Files:
  NOT modified!
  Column data still exists in files
  Just ignored during reads
  
Storage:
  NOT immediately reclaimed
  Only freed when files are rewritten (compaction)
```

### Step 4.2: Drop a Column (Demo)

```sql
-- Cell 8: Drop a column
-- =====================

-- Let's add a temporary column and then drop it
ALTER TABLE iceberg_learning.product_history
ADD COLUMN temp_column STRING COMMENT 'Temporary column for demo';

-- Verify it exists
SELECT temp_column FROM iceberg_learning.product_history LIMIT 1;

-- Now drop it
ALTER TABLE iceberg_learning.product_history
DROP COLUMN temp_column;

-- Verify it's gone
-- This will error: SELECT temp_column FROM product_history;
DESCRIBE iceberg_learning.product_history;
```

### Step 4.3: What You Cannot Do

```sql
-- These operations will FAIL:
-- ===========================

-- Cannot drop partition columns while partitioning is active
-- ALTER TABLE product_history DROP COLUMN product_id;  -- ERROR!

-- Cannot narrow types (would lose precision)
-- ALTER TABLE product_history ALTER COLUMN price TYPE INT;  -- ERROR!

-- Cannot change to incompatible types
-- ALTER TABLE product_history ALTER COLUMN price TYPE STRING;  -- ERROR!
```

---

## Step 5: Partition Evolution

### Step 5.1: Understanding Partition Evolution

```
BEFORE: Partitioned by month(valid_from)
─────────────────────────────────────────
data/
├── valid_from_month=2024-01/
│   ├── file1.parquet (Jan 1-31 data)
│   └── file2.parquet
└── valid_from_month=2024-02/
    └── file3.parquet

AFTER: ADD partition by day(valid_from)
─────────────────────────────────────────
data/
├── valid_from_month=2024-01/          ← OLD data unchanged!
│   ├── file1.parquet
│   └── file2.parquet
├── valid_from_month=2024-02/          ← OLD data unchanged!
│   └── file3.parquet
└── valid_from_day=2024-02-15/         ← NEW data uses new scheme
    └── file4.parquet

Queries work across both!
Iceberg reads metadata to understand which scheme each file uses.
```

### Step 5.2: Check Current Partition Spec

```sql
-- Cell 9: View current partitioning
-- =================================

-- Method 1: DESCRIBE EXTENDED
DESCRIBE EXTENDED iceberg_learning.product_history;

-- Look for "# Partitioning" section
-- Should show: bucket(16, product_id), month(valid_from)

-- Method 2: Query partition metadata
SELECT * FROM iceberg_learning.product_history.partitions;
```

### Step 5.3: Add Daily Partitioning

```sql
-- Cell 10: Evolve to daily partitioning
-- =====================================

-- Add day-level partitioning (finer granularity)
ALTER TABLE iceberg_learning.product_history
ADD PARTITION FIELD day(valid_from);

-- Verify new partition spec
DESCRIBE EXTENDED iceberg_learning.product_history;
```

**Expected Output (Partitioning section):**
```
# Partitioning
Part 0: bucket(16, product_id)
Part 1: month(valid_from)
Part 2: day(valid_from)        ← NEW
```

### Step 5.4: Write New Data with New Partitioning

```python
# Cell 11: Write data with new partition scheme
# =============================================

from datetime import datetime

# Create data for a specific day
day_specific_products = [
    (3001, "Daily Special 1", "Electronics", "Accessories", "DailyBrand",
     39.99, 20.00, "Special daily product", True,
     datetime(2024, 2, 15), None, True,
     datetime.now(), "daily_feed_2024_02_15.csv", "hash201",
     3, True),
    
    (3002, "Daily Special 2", "Home", "Kitchen", "DailyBrand",
     59.99, 30.00, "Another daily product", True,
     datetime(2024, 2, 15), None, True,
     datetime.now(), "daily_feed_2024_02_15.csv", "hash202",
     4, False),
]

df_daily = spark.createDataFrame(day_specific_products, columns)
df_daily = df_daily.withColumn("price", col("price").cast("decimal(10,2)")) \
                   .withColumn("cost", col("cost").cast("decimal(10,2)"))

df_daily.writeTo("iceberg_learning.product_history").append()

print("Data with new partition scheme written!")
```

### Step 5.5: Verify Mixed Partitioning

```sql
-- Cell 12: View files and their partitions
-- ========================================

SELECT 
    file_path,
    partition,
    record_count,
    file_size_in_bytes / 1024 as size_kb
FROM iceberg_learning.product_history.files
ORDER BY file_path;
```

**You should see:**
- Older files: Partitioned by month only
- Newer files: Partitioned by month AND day

### Step 5.6: Query Works Across Both

```sql
-- Cell 13: Query spans partition schemes
-- ======================================

-- This query will efficiently prune partitions
-- regardless of which partition scheme was used
SELECT 
    DATE(valid_from) as date,
    COUNT(*) as records,
    ROUND(AVG(price), 2) as avg_price
FROM iceberg_learning.product_history
WHERE valid_from >= '2024-01-01'
  AND valid_from < '2024-03-01'
GROUP BY DATE(valid_from)
ORDER BY date;
```

### Step 5.7: Drop Old Partition Field (Optional)

```sql
-- Cell 14: Remove monthly partitioning
-- ====================================

-- You can drop partition fields (new writes won't use them)
-- Old files keep their partition layout

-- WARNING: Only do this if you're sure!
-- ALTER TABLE iceberg_learning.product_history
-- DROP PARTITION FIELD month(valid_from);

-- After this:
-- - Old files: Still use month partitioning
-- - New files: Only use day partitioning
-- - Queries: Still work on both!
```

---

## Step 6: Inspect Metadata Changes

### Step 6.1: View Schema History

```sql
-- Cell 15: View schema evolution in metadata
-- ==========================================

-- Iceberg tracks all schema versions
SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary
FROM iceberg_learning.product_history.snapshots
ORDER BY committed_at DESC
LIMIT 10;
```

### Step 6.2: View Partition Spec History

```python
# Cell 16: Inspect partition specs
# ================================

# Get table metadata
table_location = spark.sql("""
    SELECT * FROM iceberg_learning.product_history.metadata_log_entries
    ORDER BY timestamp DESC
    LIMIT 1
""").collect()[0]

print(f"Latest metadata: {table_location}")

# If you have access to the metadata file directly:
# You can read the JSON to see partition-spec history
```

### Step 6.3: Compare File Layouts

```sql
-- Cell 17: Analyze file distribution
-- ==================================

-- See how files are distributed across partitions
SELECT 
    partition,
    COUNT(*) as file_count,
    SUM(record_count) as total_records,
    SUM(file_size_in_bytes) / 1024 / 1024 as total_mb
FROM iceberg_learning.product_history.files
GROUP BY partition
ORDER BY total_records DESC;
```

---

## Validation Checklist

Before moving to Phase 5, verify:

- [ ] Added `sustainability_rating` column successfully
- [ ] Old data returns NULL for new column
- [ ] New data has values for new column
- [ ] Renamed column without issues
- [ ] Added day-level partitioning
- [ ] Old and new partition schemes coexist
- [ ] Queries work across all partition schemes
- [ ] No data files were rewritten during evolution

---

## Common Issues and Solutions

### "Column already exists"

```sql
-- Check if column exists first
DESCRIBE iceberg_learning.product_history;

-- If it exists, skip the ADD
-- Or drop and re-add if needed
ALTER TABLE product_history DROP COLUMN column_name;
```

### "Cannot change partition spec"

Some partition changes require careful ordering:
```sql
-- Add new partition first
ALTER TABLE t ADD PARTITION FIELD day(ts);

-- Then optionally drop old
ALTER TABLE t DROP PARTITION FIELD month(ts);
```

### Queries getting slower after partition evolution

Mixed partition schemes may reduce pruning efficiency:
```sql
-- Consider rewriting old data to new partition scheme
-- This is optional but can improve performance

-- Option 1: Compaction (rewrites files)
-- OPTIMIZE iceberg_learning.product_history REWRITE DATA;

-- Option 2: Create new table with new partitioning
-- and migrate data
```

---

## Schema Evolution Reference

### What You CAN Do (No Rewrite)

| Operation | Command | Notes |
|-----------|---------|-------|
| Add column | `ADD COLUMN` | New column, NULLable |
| Add column with default | `ADD COLUMN ... DEFAULT` | Databricks/Spark 3.4+ |
| Drop column | `DROP COLUMN` | Data remains, ignored |
| Rename column | `RENAME COLUMN` | ID-based tracking |
| Reorder columns | `ALTER COLUMN ... AFTER` | Display order only |
| Widen type | `ALTER COLUMN TYPE` | int→long, float→double |
| Make nullable | `DROP NOT NULL` | Remove constraint |
| Add partition field | `ADD PARTITION FIELD` | New writes use it |
| Drop partition field | `DROP PARTITION FIELD` | New writes skip it |

### What You CANNOT Do

| Operation | Why Not | Workaround |
|-----------|---------|------------|
| Narrow type | Data loss | Create new column, migrate |
| Incompatible type change | Data corruption | New column + migration |
| Rename partition column | Complex metadata | Recreate table |
| Add NOT NULL to existing | Existing NULLs | Clean data first |

---

## Key Learnings from Phase 4

1. **Schema changes are metadata-only** — No data files rewritten for most operations.

2. **Column IDs are key** — Iceberg tracks columns by ID, enabling renames and reorders.

3. **Old and new coexist** — Different schemas/partitions work together seamlessly.

4. **Partition evolution is powerful** — Change strategy without rewriting history.

5. **Think before you drop** — Dropping columns doesn't free storage immediately.

---

## Next Steps

Proceed to **[Phase 5: Multi-Engine Access](07-phase5-multi-engine.md)** where you'll:

- Configure a second engine to read your data
- Prove Iceberg's engine-agnostic promise
- Understand catalog configuration across engines
- Complete the learning journey
