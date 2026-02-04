# Phase 2: Merge (Upsert) Operations

> **Objective:** Implement daily merge logic that forms the core of SCD Type 2  
> **Time Required:** 1-2 hours  
> **Prerequisites:** Completed [Phase 1](03-phase1-table-creation.md)

---

## What You'll Learn

By the end of this phase, you will:

- ✅ Understand the MERGE INTO syntax
- ✅ Implement SCD Type 2 merge logic
- ✅ Handle inserts, updates, and deletes in a single operation
- ✅ Understand copy-on-write vs merge-on-read
- ✅ Build up snapshot history through multiple merges

---

## Step 1: Understand the MERGE Operation

### What is MERGE INTO?

MERGE combines INSERT, UPDATE, and DELETE into a single atomic operation:

```
┌─────────────────────────────────────────────────────────────────────┐
│                         MERGE INTO                                   │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   SOURCE DATA              TARGET TABLE                             │
│   (Daily Feed)             (product_history)                        │
│        │                          │                                 │
│        └──────────┬───────────────┘                                 │
│                   │                                                 │
│                   ▼                                                 │
│            ┌─────────────┐                                          │
│            │  MATCH ON   │                                          │
│            │ product_id  │                                          │
│            └─────────────┘                                          │
│                   │                                                 │
│       ┌───────────┼───────────┐                                     │
│       │           │           │                                     │
│       ▼           ▼           ▼                                     │
│   ┌───────┐   ┌───────┐   ┌───────┐                                │
│   │ NEW   │   │CHANGED│   │DELETED│                                │
│   │       │   │       │   │       │                                │
│   │INSERT │   │UPDATE │   │ SOFT  │                                │
│   │ new   │   │ old   │   │DELETE │                                │
│   │record │   │+INSERT│   │       │                                │
│   │       │   │ new   │   │       │                                │
│   └───────┘   └───────┘   └───────┘                                │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Basic MERGE Syntax

```sql
MERGE INTO target_table AS target
USING source_table AS source
ON target.key = source.key

WHEN MATCHED AND <condition> THEN
    UPDATE SET col1 = value1, col2 = value2

WHEN MATCHED AND <other_condition> THEN
    DELETE

WHEN NOT MATCHED THEN
    INSERT (col1, col2, ...) VALUES (val1, val2, ...)
```

---

## Step 2: SCD Type 2 Merge Strategy

### The Logic

For SCD Type 2, when a product changes, we need to:

1. **Close the old record** — Set `valid_to` and `is_current = false`
2. **Insert the new version** — With `valid_from = today` and `is_current = true`

```
BEFORE MERGE (Product 1001 price changed from $19.99 to $24.99):
┌───────────┬───────┬────────────┬────────────┬───────────┐
│product_id │ price │ valid_from │ valid_to   │ is_current│
├───────────┼───────┼────────────┼────────────┼───────────┤
│ 1001      │ 19.99 │ 2024-01-01 │ NULL       │ true      │
└───────────┴───────┴────────────┴────────────┴───────────┘

AFTER MERGE:
┌───────────┬───────┬────────────┬────────────┬───────────┐
│product_id │ price │ valid_from │ valid_to   │ is_current│
├───────────┼───────┼────────────┼────────────┼───────────┤
│ 1001      │ 19.99 │ 2024-01-01 │ 2024-01-15 │ false     │ ← Closed
│ 1001      │ 24.99 │ 2024-01-15 │ NULL       │ true      │ ← New version
└───────────┴───────┴────────────┴────────────┴───────────┘
```

### The Challenge

Standard MERGE can either UPDATE or INSERT, but SCD Type 2 needs both for changed records. We solve this with a **two-step approach** or **INSERT-only with pre-processing**.

---

## Step 3: Generate Day 2 Changes

### Step 3.1: Create Change Simulation

```python
# Cell 1: Generate Day 2 product changes
# ======================================

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime
import random

# Load current products
current_products = spark.sql("""
    SELECT * FROM iceberg_learning.product_history
    WHERE is_current = true
""").collect()

# Simulate changes for Day 2
day2_date = datetime(2024, 1, 15, 0, 0, 0)
changes = []

# 1. Price changes (10% of products)
price_change_ids = random.sample([p.product_id for p in current_products], 100)

# 2. New products (50 new)
new_product_start_id = max(p.product_id for p in current_products) + 1

# 3. Discontinued products (20 products)
discontinued_ids = random.sample(
    [p.product_id for p in current_products if p.product_id not in price_change_ids], 
    20
)

print(f"Day 2 Changes:")
print(f"  - Price changes: {len(price_change_ids)} products")
print(f"  - New products: 50 products")
print(f"  - Discontinued: {len(discontinued_ids)} products")
```

### Step 3.2: Build the Source DataFrame

```python
# Cell 2: Build the Day 2 source data
# ===================================

# Get current products as DataFrame
df_current = spark.sql("""
    SELECT 
        product_id,
        product_name,
        category,
        subcategory,
        brand,
        price,
        cost,
        description,
        is_active
    FROM iceberg_learning.product_history
    WHERE is_current = true
""")

# Create price changes
df_price_changes = df_current.filter(col("product_id").isin(price_change_ids)) \
    .withColumn("price", spark_round(col("price") * (1 + (rand() * 0.2 - 0.1)), 2)) \
    .withColumn("change_type", lit("UPDATE"))

# Create discontinuations
df_discontinued = df_current.filter(col("product_id").isin(discontinued_ids)) \
    .withColumn("is_active", lit(False)) \
    .withColumn("change_type", lit("DELETE"))

# Create new products
categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
subcategories = ["New Arrivals", "Trending", "Premium"]
brands = ["NewBrand", "FreshStart", "Innovation"]

new_products = []
for i in range(50):
    cat = random.choice(categories)
    new_products.append({
        "product_id": new_product_start_id + i,
        "product_name": f"New Product {i+1}",
        "category": cat,
        "subcategory": random.choice(subcategories),
        "brand": random.choice(brands),
        "price": round(random.uniform(19.99, 199.99), 2),
        "cost": round(random.uniform(10.00, 100.00), 2),
        "description": f"Brand new product in {cat}",
        "is_active": True,
        "change_type": "INSERT"
    })

df_new = spark.createDataFrame(new_products)

# Unchanged products (remaining)
unchanged_ids = [p.product_id for p in current_products 
                 if p.product_id not in price_change_ids 
                 and p.product_id not in discontinued_ids]

df_unchanged = df_current.filter(col("product_id").isin(unchanged_ids)) \
    .withColumn("change_type", lit("UNCHANGED"))

# Combine all source records
df_day2_source = df_price_changes \
    .unionByName(df_discontinued) \
    .unionByName(df_new) \
    .unionByName(df_unchanged)

print(f"Day 2 source record counts:")
df_day2_source.groupBy("change_type").count().show()
```

**Expected Output:**
```
+-----------+-----+
|change_type|count|
+-----------+-----+
|     UPDATE|  100|
|     DELETE|   20|
|     INSERT|   50|
|  UNCHANGED|  830|
+-----------+-----+
```

---

## Step 4: Implement the Merge

### Understanding the Two Approaches

**Approach 1: Two-Step Merge**
- Step 1: Close changed records (UPDATE valid_to, is_current)
- Step 2: Insert new versions

**Approach 2: Single INSERT with Pre-Processing** (Recommended)
- Pre-identify changes
- Insert new records with proper SCD values
- Update existing records in same transaction

We'll use **Approach 2** as it's more efficient and idiomatic for Iceberg.

### Step 4.1: Prepare Merge Source

```python
# Cell 3: Prepare the merge source table
# ======================================

# Create temp view for merge source
day2_timestamp = datetime(2024, 1, 15, 0, 0, 0)

df_merge_source = df_day2_source \
    .withColumn("valid_from", lit(day2_timestamp)) \
    .withColumn("valid_to", lit(None).cast(TimestampType())) \
    .withColumn("is_current", lit(True)) \
    .withColumn("ingestion_timestamp", current_timestamp()) \
    .withColumn("source_file", lit("daily_feed_2024_01_15.csv")) \
    .withColumn("record_hash", md5(concat_ws("||", 
        col("product_id"), col("product_name"), col("price"), 
        col("description"), col("is_active")
    ))) \
    .withColumn("price", col("price").cast(DecimalType(10, 2))) \
    .withColumn("cost", col("cost").cast(DecimalType(10, 2)))

# Register as temp view
df_merge_source.createOrReplaceTempView("day2_source")

# Verify
spark.sql("SELECT change_type, COUNT(*) FROM day2_source GROUP BY change_type").show()
```

### Step 4.2: Execute the Merge

```sql
-- Cell 4: Execute SCD Type 2 Merge
-- ================================

-- Step 1: Close existing records that have changes
UPDATE iceberg_learning.product_history
SET 
    valid_to = TIMESTAMP '2024-01-15 00:00:00',
    is_current = false
WHERE is_current = true
AND product_id IN (
    SELECT product_id 
    FROM day2_source 
    WHERE change_type IN ('UPDATE', 'DELETE')
);
```

```sql
-- Cell 5: Insert new and updated records
-- =====================================

-- Step 2: Insert new versions of changed records + new products
INSERT INTO iceberg_learning.product_history
SELECT 
    product_id,
    product_name,
    category,
    subcategory,
    brand,
    price,
    cost,
    description,
    is_active,
    valid_from,
    valid_to,
    is_current,
    ingestion_timestamp,
    source_file,
    record_hash
FROM day2_source
WHERE change_type IN ('INSERT', 'UPDATE', 'DELETE');
```

### Step 4.3: Alternative — Single MERGE Statement

For simpler SCD patterns, you can use a single MERGE:

```sql
-- Alternative: Single MERGE (simpler but less flexible)
-- =====================================================

MERGE INTO iceberg_learning.product_history AS target
USING day2_source AS source
ON target.product_id = source.product_id AND target.is_current = true

-- When product exists and changed: close old record
WHEN MATCHED AND source.change_type IN ('UPDATE', 'DELETE') THEN
    UPDATE SET 
        valid_to = source.valid_from,
        is_current = false

-- New products
WHEN NOT MATCHED AND source.change_type = 'INSERT' THEN
    INSERT (
        product_id, product_name, category, subcategory, brand,
        price, cost, description, is_active,
        valid_from, valid_to, is_current,
        ingestion_timestamp, source_file, record_hash
    )
    VALUES (
        source.product_id, source.product_name, source.category, 
        source.subcategory, source.brand, source.price, source.cost,
        source.description, source.is_active, source.valid_from,
        source.valid_to, source.is_current, source.ingestion_timestamp,
        source.source_file, source.record_hash
    );

-- Note: After this, run INSERT for updated records (new versions)
```

---

## Step 5: Verify the Merge Results

### Step 5.1: Check Record Counts

```sql
-- Cell 6: Verify merge results
-- ============================

-- Total records (should be initial + updates + new)
SELECT 
    'Total Records' as metric,
    COUNT(*) as count
FROM iceberg_learning.product_history

UNION ALL

SELECT 
    'Current Records' as metric,
    COUNT(*) as count
FROM iceberg_learning.product_history
WHERE is_current = true

UNION ALL

SELECT 
    'Historical Records' as metric,
    COUNT(*) as count
FROM iceberg_learning.product_history
WHERE is_current = false;
```

**Expected Output:**
```
metric             | count
-------------------|-------
Total Records      | 1170   (1000 + 100 updates + 50 new + 20 discontinued)
Current Records    | 1050   (1000 - 20 discontinued + 50 new + 20 new versions)
Historical Records | 120    (100 price changes + 20 discontinued)
```

### Step 5.2: Check Specific Products

```sql
-- Cell 7: View history for specific products
-- ==========================================

-- Pick a product that had a price change
SELECT 
    product_id,
    product_name,
    price,
    is_active,
    valid_from,
    valid_to,
    is_current
FROM iceberg_learning.product_history
WHERE product_id = 1001  -- Replace with an actual changed product_id
ORDER BY valid_from;
```

**Expected Output:**
```
product_id | product_name | price | is_active | valid_from | valid_to   | is_current
-----------|--------------|-------|-----------|------------|------------|------------
1001       | Widget A     | 19.99 | true      | 2024-01-01 | 2024-01-15 | false
1001       | Widget A     | 23.49 | true      | 2024-01-15 | NULL       | true
```

### Step 5.3: Check Snapshots

```sql
-- Cell 8: View snapshots created
-- ==============================

SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary['added-records'] as added_records,
    summary['deleted-records'] as deleted_records
FROM iceberg_learning.product_history.snapshots
ORDER BY committed_at;
```

**Expected Output:**
```
snapshot_id | committed_at              | operation | added_records | deleted_records
------------|---------------------------|-----------|---------------|----------------
123...      | 2024-01-01 10:00:00.000  | append    | 1000          | 0
124...      | 2024-01-15 10:30:00.000  | overwrite | 0             | 120
125...      | 2024-01-15 10:30:15.000  | append    | 170           | 0
```

---

## Step 6: Repeat for Days 3, 4, 5

### Step 6.1: Create Reusable Merge Function

```python
# Cell 9: Create reusable merge function
# ======================================

def process_daily_feed(df_source, effective_date, source_file_name):
    """
    Process a daily product feed with SCD Type 2 logic.
    
    Args:
        df_source: DataFrame with columns [product_id, product_name, ..., change_type]
        effective_date: Date for valid_from
        source_file_name: Name of source file for lineage
    """
    
    # Add SCD columns
    df_prepared = df_source \
        .withColumn("valid_from", lit(effective_date)) \
        .withColumn("valid_to", lit(None).cast(TimestampType())) \
        .withColumn("is_current", lit(True)) \
        .withColumn("ingestion_timestamp", current_timestamp()) \
        .withColumn("source_file", lit(source_file_name)) \
        .withColumn("record_hash", md5(concat_ws("||", 
            col("product_id"), col("product_name"), col("price"), 
            col("description"), col("is_active")
        ))) \
        .withColumn("price", col("price").cast(DecimalType(10, 2))) \
        .withColumn("cost", col("cost").cast(DecimalType(10, 2)))
    
    # Get IDs to close
    close_ids = df_prepared.filter(
        col("change_type").isin(["UPDATE", "DELETE"])
    ).select("product_id").distinct().collect()
    close_id_list = [row.product_id for row in close_ids]
    
    # Step 1: Close old records
    if close_id_list:
        close_sql = f"""
            UPDATE iceberg_learning.product_history
            SET 
                valid_to = TIMESTAMP '{effective_date.strftime('%Y-%m-%d %H:%M:%S')}',
                is_current = false
            WHERE is_current = true
            AND product_id IN ({','.join(map(str, close_id_list))})
        """
        spark.sql(close_sql)
        print(f"Closed {len(close_id_list)} existing records")
    
    # Step 2: Insert new versions
    df_to_insert = df_prepared.filter(
        col("change_type").isin(["INSERT", "UPDATE", "DELETE"])
    ).drop("change_type")
    
    insert_count = df_to_insert.count()
    if insert_count > 0:
        df_to_insert.writeTo("iceberg_learning.product_history").append()
        print(f"Inserted {insert_count} new records")
    
    return insert_count, len(close_id_list)

print("Function created successfully!")
```

### Step 6.2: Process Day 3

```python
# Cell 10: Process Day 3
# ======================

# Generate Day 3 changes (similar pattern)
day3_date = datetime(2024, 1, 20, 0, 0, 0)

# Get current state
df_current = spark.sql("""
    SELECT * FROM iceberg_learning.product_history WHERE is_current = true
""")

# Simulate: 80 price changes, 30 new products, 15 discontinued
current_ids = [row.product_id for row in df_current.select("product_id").collect()]
price_change_ids = random.sample(current_ids, 80)
remaining = [x for x in current_ids if x not in price_change_ids]
discontinued_ids = random.sample(remaining, 15)

# ... (similar data generation code as Day 2)
# For brevity, using a simplified version:

df_day3_changes = df_current.filter(col("product_id").isin(price_change_ids[:80])) \
    .withColumn("price", spark_round(col("price") * 1.05, 2)) \
    .withColumn("change_type", lit("UPDATE"))

inserted, closed = process_daily_feed(df_day3_changes, day3_date, "daily_feed_2024_01_20.csv")
print(f"Day 3 complete: {inserted} inserted, {closed} closed")
```

### Step 6.3: Verify Multiple Snapshots

```sql
-- Cell 11: View all snapshots
-- ===========================

SELECT 
    ROW_NUMBER() OVER (ORDER BY committed_at) as snapshot_num,
    snapshot_id,
    committed_at,
    operation,
    summary['added-records'] as added,
    summary['deleted-records'] as deleted
FROM iceberg_learning.product_history.snapshots
ORDER BY committed_at;
```

---

## Step 7: Understand Copy-on-Write vs Merge-on-Read

### The Two Strategies

| Aspect | Copy-on-Write (COW) | Merge-on-Read (MOR) |
|--------|---------------------|---------------------|
| **On UPDATE/DELETE** | Rewrites entire data file | Writes small delete file |
| **Write speed** | Slower | Faster |
| **Read speed** | Faster | Slower (must apply deletes) |
| **Storage during write** | Temporarily doubles | Minimal overhead |
| **Best for** | Read-heavy workloads | Write-heavy workloads |

### Visual Comparison

```
COPY-ON-WRITE (Default):
─────────────────────────
                     DELETE row 5
Original File           │           New File
┌─────────────┐         │         ┌─────────────┐
│ Row 1       │         │         │ Row 1       │
│ Row 2       │         │         │ Row 2       │
│ Row 3       │    ─────┼────▶    │ Row 3       │
│ Row 4       │         │         │ Row 4       │
│ Row 5 ←───────────────┘         │ Row 6       │  (Row 5 gone)
│ Row 6       │                   │ Row 7       │
│ Row 7       │                   └─────────────┘
└─────────────┘                   
                                  Original file deleted

MERGE-ON-READ:
──────────────
                     DELETE row 5
Original File           │           Delete File
┌─────────────┐         │         ┌─────────────┐
│ Row 1       │         │         │ delete: 5   │
│ Row 2       │         │         └─────────────┘
│ Row 3       │    ─────┼────▶    
│ Row 4       │         │         On READ:
│ Row 5       │◄────────┘         Apply delete file
│ Row 6       │                   to skip Row 5
│ Row 7       │                   
└─────────────┘                   
                                  Original file kept!
```

### Check Current Mode

```sql
-- Cell 12: Check write mode
-- =========================

SHOW TBLPROPERTIES iceberg_learning.product_history;

-- Look for:
-- write.delete.mode = copy-on-write (or merge-on-read)
-- write.update.mode = copy-on-write (or merge-on-read)
```

### Change Write Mode (Optional)

```sql
-- Switch to merge-on-read for faster writes
ALTER TABLE iceberg_learning.product_history
SET TBLPROPERTIES (
    'write.delete.mode' = 'merge-on-read',
    'write.update.mode' = 'merge-on-read'
);

-- Switch back to copy-on-write for faster reads
ALTER TABLE iceberg_learning.product_history
SET TBLPROPERTIES (
    'write.delete.mode' = 'copy-on-write',
    'write.update.mode' = 'copy-on-write'
);
```

---

## Validation Checklist

Before moving to Phase 3, verify:

- [ ] Day 2 merge completed successfully
- [ ] Changed products show two records (old + new)
- [ ] `is_current` flag correctly identifies current records
- [ ] `valid_to` populated for closed records
- [ ] Multiple snapshots visible in metadata
- [ ] Understand COW vs MOR tradeoff
- [ ] Can query current state and full history

---

## Common Issues and Solutions

### "Duplicate key" errors

Your source may have duplicates:
```sql
-- Check for duplicates in source
SELECT product_id, COUNT(*) 
FROM day2_source 
GROUP BY product_id 
HAVING COUNT(*) > 1;
```

### Records not closing properly

Ensure you're matching on `is_current = true`:
```sql
-- Only close CURRENT records
UPDATE ... WHERE is_current = true AND ...
```

### Wrong record counts after merge

Check change_type categorization:
```python
# Debug: Show all change types
df_day2_source.groupBy("change_type").count().show()
```

---

## Key Learnings from Phase 2

1. **SCD Type 2 requires two steps** — Close old records, insert new versions.

2. **MERGE is powerful but limited** — For complex SCD logic, UPDATE + INSERT may be clearer.

3. **Every operation creates snapshots** — You can see exactly what happened in metadata.

4. **Write mode affects performance** — Choose COW for reads, MOR for writes.

5. **Idempotency matters** — Design merges to be safely re-runnable.

---

## Next Steps

Proceed to **[Phase 3: Time Travel and Auditing](05-phase3-time-travel.md)** where you'll:

- Query historical states
- Build change detection queries
- Create audit reports
- Manage snapshot retention
