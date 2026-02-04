# Phase 3: Time Travel and Auditing

> **Objective:** Query historical states and build an audit trail  
> **Time Required:** 1-2 hours  
> **Prerequisites:** Completed [Phase 2](04-phase2-merge-operations.md) with multiple snapshots

---

## What You'll Learn

By the end of this phase, you will:

- ✅ Query data "as of" any point in time
- ✅ Compare snapshots to detect changes
- ✅ Build audit reports showing what changed
- ✅ Manage snapshot retention
- ✅ Understand storage cost implications

---

## Step 1: Explore Your Snapshots

### Step 1.1: List All Snapshots

```sql
-- Cell 1: View all snapshots
-- ==========================

SELECT 
    snapshot_id,
    committed_at,
    operation,
    summary['added-records'] as added_records,
    summary['deleted-records'] as deleted_records,
    summary['total-records'] as total_records,
    summary['added-files-size'] as added_bytes
FROM iceberg_learning.product_history.snapshots
ORDER BY committed_at;
```

**Expected Output (example):**
```
snapshot_id      | committed_at              | operation | added_records | deleted_records
-----------------|---------------------------|-----------|---------------|----------------
847293847293     | 2024-01-01 10:00:00.000  | append    | 1000          | null
947382947382     | 2024-01-15 10:30:00.000  | overwrite | null          | 120
957483957483     | 2024-01-15 10:30:15.000  | append    | 170           | null
967584967584     | 2024-01-20 09:00:00.000  | overwrite | null          | 80
977685977685     | 2024-01-20 09:00:10.000  | append    | 80            | null
```

### Step 1.2: View Snapshot History

```sql
-- Cell 2: View history with ancestors
-- ===================================

SELECT 
    made_current_at,
    snapshot_id,
    parent_id,
    is_current_ancestor
FROM iceberg_learning.product_history.history
ORDER BY made_current_at;
```

### Step 1.3: Save Snapshot IDs for Later

```python
# Cell 3: Get snapshot IDs for time travel
# ========================================

snapshots = spark.sql("""
    SELECT snapshot_id, committed_at 
    FROM iceberg_learning.product_history.snapshots
    ORDER BY committed_at
""").collect()

print("Available snapshots:")
for i, snap in enumerate(snapshots):
    print(f"  [{i}] Snapshot {snap.snapshot_id} at {snap.committed_at}")

# Store for later use
SNAPSHOT_INITIAL = snapshots[0].snapshot_id
SNAPSHOT_DAY2 = snapshots[-2].snapshot_id if len(snapshots) > 1 else None
SNAPSHOT_LATEST = snapshots[-1].snapshot_id

print(f"\nKey snapshots:")
print(f"  Initial load: {SNAPSHOT_INITIAL}")
print(f"  Latest: {SNAPSHOT_LATEST}")
```

---

## Step 2: Time Travel Queries

### The Three Ways to Time Travel

```
┌─────────────────────────────────────────────────────────────────────┐
│                    TIME TRAVEL OPTIONS                               │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  1. BY SNAPSHOT ID                                                  │
│     SELECT * FROM table VERSION AS OF 12345678                      │
│     → Exact snapshot, most precise                                  │
│                                                                     │
│  2. BY TIMESTAMP                                                    │
│     SELECT * FROM table TIMESTAMP AS OF '2024-01-15 10:00:00'      │
│     → Returns state at that moment                                  │
│                                                                     │
│  3. BY RELATIVE TIME (Databricks SQL)                              │
│     SELECT * FROM table TIMESTAMP AS OF current_timestamp() - INTERVAL 7 DAYS│
│     → Good for "last week" queries                                  │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Step 2.1: Query by Snapshot ID

```sql
-- Cell 4: Query specific snapshot
-- ===============================

-- Query initial load state
SELECT 
    'Initial Load' as snapshot_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT product_id) as unique_products,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records
FROM iceberg_learning.product_history
VERSION AS OF 847293847293;  -- Replace with your SNAPSHOT_INITIAL

-- Compare with latest
SELECT 
    'Latest' as snapshot_name,
    COUNT(*) as record_count,
    COUNT(DISTINCT product_id) as unique_products,
    SUM(CASE WHEN is_current THEN 1 ELSE 0 END) as current_records
FROM iceberg_learning.product_history;
```

### Step 2.2: Query by Timestamp

```sql
-- Cell 5: Query by timestamp
-- ==========================

-- What did the data look like on January 10th?
SELECT 
    COUNT(*) as total_records,
    ROUND(AVG(price), 2) as avg_price,
    COUNT(DISTINCT category) as categories
FROM iceberg_learning.product_history
TIMESTAMP AS OF '2024-01-10 00:00:00';

-- What about January 16th (after Day 2 merge)?
SELECT 
    COUNT(*) as total_records,
    ROUND(AVG(price), 2) as avg_price,
    COUNT(DISTINCT category) as categories
FROM iceberg_learning.product_history
TIMESTAMP AS OF '2024-01-16 00:00:00';
```

### Step 2.3: Track a Specific Product Over Time

```sql
-- Cell 6: Product history over time
-- =================================

-- Get a product that changed
WITH changed_products AS (
    SELECT DISTINCT product_id
    FROM iceberg_learning.product_history
    WHERE is_current = false
    LIMIT 1
)
SELECT 
    h.product_id,
    h.product_name,
    h.price,
    h.valid_from,
    h.valid_to,
    h.is_current
FROM iceberg_learning.product_history h
JOIN changed_products cp ON h.product_id = cp.product_id
ORDER BY h.valid_from;
```

---

## Step 3: Build Change Detection Queries

### Step 3.1: Compare Two Snapshots

```sql
-- Cell 7: Compare snapshots to find changes
-- =========================================

-- Create views for before and after states
CREATE OR REPLACE TEMP VIEW before_state AS
SELECT * FROM iceberg_learning.product_history
VERSION AS OF 847293847293;  -- Initial snapshot

CREATE OR REPLACE TEMP VIEW after_state AS
SELECT * FROM iceberg_learning.product_history
VERSION AS OF 977685977685;  -- Latest snapshot (replace with yours)

-- Find all changes
SELECT 
    COALESCE(a.product_id, b.product_id) as product_id,
    CASE 
        WHEN b.product_id IS NULL THEN 'DELETED'
        WHEN a.product_id IS NULL THEN 'ADDED'
        ELSE 'MODIFIED'
    END as change_type,
    a.price as old_price,
    b.price as new_price,
    a.is_active as was_active,
    b.is_active as is_active
FROM before_state a
FULL OUTER JOIN after_state b 
    ON a.product_id = b.product_id 
    AND a.is_current = true 
    AND b.is_current = true
WHERE a.product_id IS NULL 
   OR b.product_id IS NULL
   OR a.price != b.price
   OR a.is_active != b.is_active
ORDER BY change_type, product_id;
```

### Step 3.2: Price Change Analysis

```sql
-- Cell 8: Analyze price changes
-- =============================

WITH price_history AS (
    SELECT 
        product_id,
        product_name,
        category,
        price,
        valid_from,
        LAG(price) OVER (PARTITION BY product_id ORDER BY valid_from) as previous_price
    FROM iceberg_learning.product_history
)
SELECT 
    product_id,
    product_name,
    category,
    previous_price,
    price as current_price,
    price - previous_price as price_change,
    ROUND((price - previous_price) / previous_price * 100, 2) as pct_change,
    valid_from as changed_on
FROM price_history
WHERE previous_price IS NOT NULL
  AND price != previous_price
ORDER BY ABS(price - previous_price) DESC
LIMIT 20;
```

**Expected Output:**
```
product_id | product_name     | previous_price | current_price | price_change | pct_change | changed_on
-----------|------------------|----------------|---------------|--------------|------------|------------
1234       | Premium Widget   | 199.99         | 219.99        | 20.00        | 10.00      | 2024-01-15
1567       | Budget Gadget    | 29.99          | 24.99         | -5.00        | -16.67     | 2024-01-15
...
```

---

## Step 4: Build Audit Reports

### Step 4.1: Daily Change Summary

```sql
-- Cell 9: Daily change summary view
-- =================================

CREATE OR REPLACE VIEW iceberg_learning.v_daily_changes AS
SELECT 
    DATE(valid_from) as change_date,
    COUNT(DISTINCT product_id) as products_affected,
    
    -- New products
    SUM(CASE 
        WHEN is_current = true 
        AND NOT EXISTS (
            SELECT 1 FROM iceberg_learning.product_history h2 
            WHERE h2.product_id = product_history.product_id 
            AND h2.valid_from < product_history.valid_from
        )
        THEN 1 ELSE 0 
    END) as new_products,
    
    -- Price changes
    SUM(CASE 
        WHEN is_current = false 
        THEN 1 ELSE 0 
    END) as records_superseded,
    
    -- Discontinued
    SUM(CASE 
        WHEN is_active = false AND is_current = true
        THEN 1 ELSE 0 
    END) as discontinued_products
    
FROM iceberg_learning.product_history
GROUP BY DATE(valid_from)
ORDER BY change_date;

-- Query the view
SELECT * FROM iceberg_learning.v_daily_changes;
```

### Step 4.2: Product Audit Trail

```sql
-- Cell 10: Complete audit trail for a product
-- ===========================================

CREATE OR REPLACE VIEW iceberg_learning.v_product_audit AS
SELECT 
    product_id,
    product_name,
    category,
    price,
    is_active,
    valid_from,
    valid_to,
    is_current,
    source_file,
    CASE 
        WHEN valid_to IS NULL THEN 'CURRENT'
        WHEN is_active = false THEN 'DISCONTINUED'
        ELSE 'SUPERSEDED'
    END as status,
    ROW_NUMBER() OVER (PARTITION BY product_id ORDER BY valid_from) as version_num
FROM iceberg_learning.product_history;

-- Example: View audit trail for specific product
SELECT * 
FROM iceberg_learning.v_product_audit
WHERE product_id = 1001  -- Replace with actual product_id
ORDER BY version_num;
```

### Step 4.3: Snapshot Comparison Report

```python
# Cell 11: Generate snapshot comparison report
# ============================================

def compare_snapshots(snapshot_before, snapshot_after):
    """
    Compare two snapshots and return change statistics.
    """
    
    # Get before state
    df_before = spark.sql(f"""
        SELECT product_id, price, is_active, record_hash
        FROM iceberg_learning.product_history
        VERSION AS OF {snapshot_before}
        WHERE is_current = true
    """)
    
    # Get after state
    df_after = spark.sql(f"""
        SELECT product_id, price, is_active, record_hash
        FROM iceberg_learning.product_history
        VERSION AS OF {snapshot_after}
        WHERE is_current = true
    """)
    
    # Register as temp views
    df_before.createOrReplaceTempView("snap_before")
    df_after.createOrReplaceTempView("snap_after")
    
    # Compare
    comparison = spark.sql("""
        SELECT 
            -- Counts
            (SELECT COUNT(*) FROM snap_before) as before_count,
            (SELECT COUNT(*) FROM snap_after) as after_count,
            
            -- Added (in after but not in before)
            (SELECT COUNT(*) 
             FROM snap_after a 
             WHERE NOT EXISTS (SELECT 1 FROM snap_before b WHERE b.product_id = a.product_id)
            ) as added_count,
            
            -- Removed (in before but not in after)
            (SELECT COUNT(*) 
             FROM snap_before b 
             WHERE NOT EXISTS (SELECT 1 FROM snap_after a WHERE a.product_id = b.product_id)
            ) as removed_count,
            
            -- Modified (hash changed)
            (SELECT COUNT(*) 
             FROM snap_before b 
             JOIN snap_after a ON b.product_id = a.product_id
             WHERE b.record_hash != a.record_hash
            ) as modified_count
    """)
    
    return comparison

# Run comparison
result = compare_snapshots(SNAPSHOT_INITIAL, SNAPSHOT_LATEST)
result.show()
```

---

## Step 5: Snapshot Management

### Understanding Snapshot Lifecycle

```
┌─────────────────────────────────────────────────────────────────────┐
│                    SNAPSHOT LIFECYCLE                                │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  ACTIVE                EXPIRED               CLEANED UP             │
│  ┌──────┐              ┌──────┐              ┌──────┐              │
│  │ ✓    │   expire     │ ✗    │   remove     │      │              │
│  │ Can  │ ──────────▶  │Cannot│ ──────────▶  │ Gone │              │
│  │query │   snapshots  │query │   orphaned   │      │              │
│  │      │              │      │   files      │      │              │
│  └──────┘              └──────┘              └──────┘              │
│                                                                     │
│  Time travel: ✓        Time travel: ✗        Time travel: ✗        │
│  Storage: Active       Storage: Orphaned     Storage: Freed        │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Step 5.1: Check Current Retention Settings

```sql
-- Cell 12: Check snapshot retention settings
-- ==========================================

SHOW TBLPROPERTIES iceberg_learning.product_history;

-- Look for:
-- history.expire.max-snapshot-age-ms (default: 5 days = 432000000 ms)
-- history.expire.min-snapshots-to-keep (default: 1)
```

### Step 5.2: View Snapshot Storage Usage

```sql
-- Cell 13: Estimate storage per snapshot
-- ======================================

SELECT 
    snapshot_id,
    committed_at,
    CAST(summary['added-files-size'] AS BIGINT) / 1024 / 1024 as added_mb,
    CAST(summary['total-data-files'] AS INT) as file_count
FROM iceberg_learning.product_history.snapshots
ORDER BY committed_at;
```

### Step 5.3: Expire Old Snapshots

```sql
-- Cell 14: Expire snapshots older than a date
-- ===========================================

-- WARNING: This is irreversible! Old snapshots become unqueryable.

-- Check which snapshots would be affected first
SELECT snapshot_id, committed_at 
FROM iceberg_learning.product_history.snapshots
WHERE committed_at < '2024-01-10 00:00:00';

-- Then expire them (uncomment when ready)
-- CALL iceberg_learning.system.expire_snapshots(
--     table => 'product_history',
--     older_than => TIMESTAMP '2024-01-10 00:00:00',
--     retain_last => 2
-- );
```

### Step 5.4: Remove Orphaned Files

```sql
-- Cell 15: Clean up orphaned files
-- ================================

-- After expiring snapshots, old data files become "orphaned"
-- They're no longer referenced but still take storage

-- Find orphaned files (Databricks)
-- CALL iceberg_learning.system.remove_orphan_files(
--     table => 'product_history',
--     older_than => TIMESTAMP '2024-01-10 00:00:00'
-- );
```

### Step 5.5: Set Retention Policy

```sql
-- Cell 16: Configure retention policy
-- ===================================

-- Keep snapshots for 30 days
ALTER TABLE iceberg_learning.product_history
SET TBLPROPERTIES (
    'history.expire.max-snapshot-age-ms' = '2592000000',  -- 30 days
    'history.expire.min-snapshots-to-keep' = '5'
);
```

---

## Step 6: Rollback Capabilities

### Step 6.1: Rollback to Previous Snapshot

```sql
-- Cell 17: Rollback to previous state
-- ===================================

-- WARNING: This changes the current state of the table!

-- First, verify which snapshot you want
SELECT snapshot_id, committed_at, operation
FROM iceberg_learning.product_history.snapshots
ORDER BY committed_at;

-- Rollback (uncomment when ready)
-- CALL iceberg_learning.system.rollback_to_snapshot(
--     'product_history',
--     847293847293  -- Replace with target snapshot_id
-- );

-- After rollback, latest changes are "undone"
-- But a new snapshot is created (recording the rollback)
```

### Step 6.2: Cherry-Pick Specific Snapshot

```sql
-- Cell 18: Cherry-pick from old snapshot
-- ======================================

-- Create a new table from an old snapshot state
-- (Useful for point-in-time analysis)

CREATE TABLE iceberg_learning.product_history_jan10
USING ICEBERG
AS
SELECT * 
FROM iceberg_learning.product_history
TIMESTAMP AS OF '2024-01-10 00:00:00';
```

---

## Validation Checklist

Before moving to Phase 4, verify:

- [ ] Can query any snapshot by ID
- [ ] Can query by timestamp
- [ ] Change detection queries work
- [ ] Audit views created successfully
- [ ] Understand snapshot retention settings
- [ ] Know how to expire old snapshots
- [ ] Understand storage implications

---

## Common Issues and Solutions

### "Snapshot not found"

The snapshot may have been expired:
```sql
-- Check available snapshots
SELECT snapshot_id, committed_at 
FROM iceberg_learning.product_history.snapshots;
```

### Timestamp doesn't return expected data

Timestamps are resolved to the snapshot active at that time:
```sql
-- Find exact snapshot for a timestamp
SELECT snapshot_id, committed_at
FROM iceberg_learning.product_history.snapshots
WHERE committed_at <= TIMESTAMP '2024-01-15 10:00:00'
ORDER BY committed_at DESC
LIMIT 1;
```

### Storage keeps growing

Snapshots retain old files. Implement regular expiration:
```sql
-- Automate expiration (run weekly)
CALL system.expire_snapshots('product_history', older_than => current_timestamp() - INTERVAL 7 DAYS);
```

---

## Storage Cost Analysis

### Calculate Storage Over Time

```python
# Cell 19: Storage analysis
# =========================

# Get file info
files_df = spark.sql("""
    SELECT 
        file_path,
        file_size_in_bytes,
        record_count,
        partition
    FROM iceberg_learning.product_history.files
""")

total_bytes = files_df.agg({"file_size_in_bytes": "sum"}).collect()[0][0]
total_files = files_df.count()
total_records = spark.sql("SELECT COUNT(*) FROM iceberg_learning.product_history").collect()[0][0]

print(f"Storage Summary:")
print(f"  Total files: {total_files}")
print(f"  Total storage: {total_bytes / 1024 / 1024:.2f} MB")
print(f"  Total records: {total_records:,}")
print(f"  Avg file size: {total_bytes / total_files / 1024:.2f} KB")
print(f"  Avg bytes/record: {total_bytes / total_records:.2f}")
```

### Snapshot Storage Breakdown

```python
# Cell 20: Storage per snapshot
# =============================

# This shows incremental storage added by each snapshot
storage_df = spark.sql("""
    SELECT 
        snapshot_id,
        committed_at,
        CAST(summary['added-files-size'] AS BIGINT) as added_bytes,
        CAST(summary['removed-files-size'] AS BIGINT) as removed_bytes
    FROM iceberg_learning.product_history.snapshots
    ORDER BY committed_at
""")

storage_df.show()
```

---

## Key Learnings from Phase 3

1. **Time travel is powerful** — Query any point in time for auditing, debugging, or analysis.

2. **Snapshots have costs** — Each snapshot retains file references; manage retention carefully.

3. **Expiration is irreversible** — Once expired, that snapshot is gone forever.

4. **Two-step cleanup** — First expire snapshots, then remove orphaned files.

5. **Audit trails are built-in** — Metadata tables provide complete operation history.

---

## Next Steps

Proceed to **[Phase 4: Schema and Partition Evolution](06-phase4-evolution.md)** where you'll:

- Add new columns without rewriting data
- Change partition strategy on live table
- See how old and new schemas coexist
- Understand metadata evolution
