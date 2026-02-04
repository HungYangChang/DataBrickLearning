# Understanding Apache Iceberg

> **Reading Time:** 20-30 minutes  
> **Prerequisites:** Basic SQL knowledge, familiarity with data warehousing concepts

---

## What is Apache Iceberg?

Apache Iceberg is an **open table format** designed for huge analytic datasets. Think of it as a smart layer that sits between:

```
┌─────────────────────────────────────────────────────────┐
│              COMPUTE ENGINES                             │
│    Spark  •  Trino  •  Flink  •  Snowflake  •  Athena   │
└─────────────────────────────────────────────────────────┘
                          ▲
                          │
┌─────────────────────────────────────────────────────────┐
│              APACHE ICEBERG                              │
│         (Table Format + Metadata Layer)                  │
└─────────────────────────────────────────────────────────┘
                          ▲
                          │
┌─────────────────────────────────────────────────────────┐
│              CLOUD STORAGE                               │
│         S3  •  ADLS  •  GCS  •  HDFS                    │
└─────────────────────────────────────────────────────────┘
```

---

## Why Was Iceberg Created?

Traditional data lakes have serious problems:

| Problem | Traditional Data Lake | With Iceberg |
|---------|----------------------|--------------|
| **Transactions** | No ACID guarantees | Full ACID support |
| **Schema Changes** | Requires data rewrite | No rewrite needed |
| **Time Travel** | Not possible | Query any past state |
| **Partition Changes** | Painful, requires rewrite | Seamless evolution |
| **Concurrent Writes** | Conflicts & corruption | Safe concurrent access |
| **Query Planning** | Scan all files | Smart file skipping |

---

## The Metadata Architecture

> ⭐ **This is the most important concept to understand.**

An Iceberg table is NOT just a pile of Parquet files. It has a hierarchical metadata structure that enables all its powerful features.

### The Hierarchy (Top to Bottom)

```
LEVEL 1: CATALOG
    │
    │   "Where is table X?"
    │   Maps table names → metadata file locations
    │
    ▼
LEVEL 2: METADATA FILE (JSON)
    │
    │   "What does this table look like?"
    │   • Schema definition
    │   • Partition specification  
    │   • Current snapshot pointer
    │   • Table properties
    │
    ▼
LEVEL 3: SNAPSHOT
    │
    │   "What was the table state at this moment?"
    │   • Unique snapshot ID
    │   • Timestamp
    │   • Operation type (append, overwrite, delete)
    │   • Pointer to manifest list
    │
    ▼
LEVEL 4: MANIFEST LIST (Avro file)
    │
    │   "Which manifest files make up this snapshot?"
    │   • List of manifest file paths
    │   • Partition summaries for quick pruning
    │   • File counts
    │
    ▼
LEVEL 5: MANIFEST FILES (Avro files)
    │
    │   "Which data files contain my data?"
    │   • Data file paths
    │   • Partition values
    │   • Column statistics (min, max, null count)
    │   • Record counts
    │
    ▼
LEVEL 6: DATA FILES (Parquet, ORC, or Avro)

    The actual data rows
```

### Visual Example

```
my_catalog.my_database.products
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│  metadata/v3.metadata.json                                │
│  ┌────────────────────────────────────────────────────┐  │
│  │ "current-snapshot-id": 789                          │  │
│  │ "schemas": [...]                                    │  │
│  │ "partition-specs": [...]                            │  │
│  │ "snapshots": [                                      │  │
│  │   { "snapshot-id": 789, "manifest-list": "snap-789"}│  │
│  │   { "snapshot-id": 456, "manifest-list": "snap-456"}│  │
│  │ ]                                                   │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
           │
           ▼ (current snapshot)
┌──────────────────────────────────────────────────────────┐
│  snap-789-manifest-list.avro                              │
│  ┌────────────────────────────────────────────────────┐  │
│  │ manifest-1.avro → partition=Electronics, 50 files  │  │
│  │ manifest-2.avro → partition=Clothing, 30 files     │  │
│  │ manifest-3.avro → partition=Home, 25 files         │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│  manifest-1.avro                                          │
│  ┌────────────────────────────────────────────────────┐  │
│  │ data/part-00001.parquet                             │  │
│  │   → partition: Electronics                          │  │
│  │   → records: 10,000                                 │  │
│  │   → price_min: 9.99, price_max: 999.99             │  │
│  │                                                     │  │
│  │ data/part-00002.parquet                             │  │
│  │   → partition: Electronics                          │  │
│  │   → records: 8,500                                  │  │
│  │   → price_min: 14.99, price_max: 1299.99           │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────────────────────┐
│  data/part-00001.parquet                                  │
│  ┌────────────────────────────────────────────────────┐  │
│  │ id=1, name="Laptop", category="Electronics"...     │  │
│  │ id=2, name="Phone", category="Electronics"...      │  │
│  │ ...                                                 │  │
│  └────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────┘
```

### Why This Structure Matters

| Benefit | How Metadata Enables It |
|---------|------------------------|
| **Atomic commits** | New snapshot only visible after metadata file update completes |
| **Time travel** | Old snapshots retained, each points to its own manifest list |
| **Fast query planning** | Column stats in manifests enable file skipping without reading data |
| **Partition pruning** | Partition info in manifests lets engine skip irrelevant files |
| **Concurrent safety** | Optimistic concurrency on metadata file updates |

---

## Snapshots and Time Travel

### What is a Snapshot?

Every write operation creates a new **snapshot**—an immutable point-in-time view of your table.

```
Timeline:
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━▶

   Snapshot 1        Snapshot 2        Snapshot 3        Snapshot 4
   (Initial)         (+50 rows)        (-10 rows)        (+column)
       │                 │                 │                 │
   Jan 1st           Jan 2nd           Jan 3rd           Jan 4th
       │                 │                 │                 │
       ▼                 ▼                 ▼                 ▼
   ┌───────┐         ┌───────┐         ┌───────┐         ┌───────┐
   │ 1000  │         │ 1050  │         │ 1040  │         │ 1040  │
   │ rows  │         │ rows  │         │ rows  │         │ rows  │
   └───────┘         └───────┘         └───────┘         └───────┘
```

### Operations That Create Snapshots

| Operation | What Happens |
|-----------|--------------|
| `INSERT INTO` | Adds new data files, creates snapshot |
| `UPDATE` | Rewrites affected files, creates snapshot |
| `DELETE` | Marks rows deleted or rewrites files, creates snapshot |
| `MERGE INTO` | Combination of insert/update/delete, creates snapshot |
| `OPTIMIZE` / Compaction | Rewrites small files into larger ones, creates snapshot |

### Time Travel Queries

```sql
-- Query the current state
SELECT * FROM products;

-- Query as of a specific timestamp
SELECT * FROM products 
TIMESTAMP AS OF '2024-01-15 10:00:00';

-- Query as of a specific snapshot ID
SELECT * FROM products 
VERSION AS OF 1234567890;

-- Compare two points in time
SELECT 
    'before' as state, COUNT(*) 
FROM products VERSION AS OF 100
UNION ALL
SELECT 
    'after' as state, COUNT(*) 
FROM products VERSION AS OF 200;
```

### Snapshot Retention

```
┌─────────────────────────────────────────────────────────────┐
│                    SNAPSHOT LIFECYCLE                        │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│   Active ──────────▶ Expired ──────────▶ Deleted           │
│                                                             │
│   • Queryable         • Not queryable     • Gone forever   │
│   • Takes storage     • Files orphaned    • Storage freed  │
│   • Time travel OK    • Awaiting cleanup                   │
│                                                             │
└─────────────────────────────────────────────────────────────┘

Default retention: varies by implementation (often 5-7 days)
```

**Key Tradeoff:**
- Longer retention = More time travel capability, but higher storage cost
- Shorter retention = Lower storage cost, but limited historical access

---

## Hidden Partitioning

### The Problem with Traditional Partitioning

In Hive-style partitioning, you must:
1. Know the exact partition columns
2. Write queries with explicit partition filters
3. Rewrite ALL data if you want to change partitioning

```sql
-- Hive style: You MUST know the partition structure
SELECT * FROM products 
WHERE year = 2024 AND month = 1 AND day = 15;

-- Without partition filter: FULL TABLE SCAN! 😱
SELECT * FROM products 
WHERE order_date = '2024-01-15';
```

### Iceberg's Solution: Partition Transforms

Iceberg stores partition information in metadata. You write normal queries, and Iceberg automatically prunes partitions.

```sql
-- Iceberg: Just write normal predicates
SELECT * FROM products 
WHERE order_date = '2024-01-15';

-- Iceberg reads metadata, sees partition transform is day(order_date),
-- and automatically prunes to only the relevant partition!
```

### Available Partition Transforms

| Transform | Input | Output | Use Case |
|-----------|-------|--------|----------|
| `identity(col)` | value | same value | Low-cardinality columns |
| `year(ts)` | 2024-03-15 | 2024 | Yearly analysis |
| `month(ts)` | 2024-03-15 | 2024-03 | Monthly analysis |
| `day(ts)` | 2024-03-15 | 2024-03-15 | Daily analysis |
| `hour(ts)` | 2024-03-15 14:30 | 2024-03-15-14 | Hourly analysis |
| `bucket(N, col)` | any value | 0 to N-1 | Distribute evenly |
| `truncate(L, col)` | "Electronics" | "Elec" (if L=4) | String grouping |

### Visual Example

```
Table Definition:
  PARTITIONED BY (month(order_date), bucket(16, customer_id))

Data Layout on Storage:
┌─────────────────────────────────────────────────────────────┐
│ s3://bucket/warehouse/orders/                                │
│                                                             │
│ ├── order_date_month=2024-01/                               │
│ │   ├── customer_id_bucket=0/                               │
│ │   │   └── part-00001.parquet                              │
│ │   ├── customer_id_bucket=1/                               │
│ │   │   └── part-00002.parquet                              │
│ │   └── ...                                                 │
│ │                                                           │
│ ├── order_date_month=2024-02/                               │
│ │   ├── customer_id_bucket=0/                               │
│ │   └── ...                                                 │
│ └── ...                                                     │
└─────────────────────────────────────────────────────────────┘

Query: WHERE order_date = '2024-01-15' AND customer_id = 12345
  → Iceberg computes: month = 2024-01, bucket = 7
  → Only reads files in: order_date_month=2024-01/customer_id_bucket=7/
```

---

## Schema Evolution

### The Problem with Traditional Formats

Changing schema in traditional formats often means:
- Rewriting the entire table
- Downtime during migration
- Complex ETL to handle old vs new data

### How Iceberg Handles Schema Changes

Iceberg tracks columns by **unique numeric IDs**, not by name or position.

```
Original Schema:                    After Evolution:
┌────┬────────────┬──────────┐     ┌────┬──────────────────┬──────────┐
│ ID │ Name       │ Type     │     │ ID │ Name             │ Type     │
├────┼────────────┼──────────┤     ├────┼──────────────────┼──────────┤
│ 1  │ product_id │ long     │     │ 1  │ product_id       │ long     │
│ 2  │ name       │ string   │     │ 2  │ product_name     │ string   │ ← Renamed
│ 3  │ price      │ decimal  │     │ 3  │ price            │ decimal  │
│ 4  │ category   │ string   │     │ 5  │ rating           │ double   │ ← Added
└────┴────────────┴──────────┘     │ -- │ (category)       │ dropped  │ ← Dropped
                                   └────┴──────────────────┴──────────┘
```

### Supported Schema Changes (No Data Rewrite!)

| Change | Command | Notes |
|--------|---------|-------|
| **Add column** | `ALTER TABLE t ADD COLUMN col TYPE` | Old files return NULL for new column |
| **Drop column** | `ALTER TABLE t DROP COLUMN col` | Data remains, just hidden from queries |
| **Rename column** | `ALTER TABLE t RENAME COLUMN old TO new` | ID-based tracking handles this |
| **Reorder columns** | `ALTER TABLE t ALTER COLUMN col AFTER other` | Display order change only |
| **Widen type** | `ALTER TABLE t ALTER COLUMN col TYPE newtype` | int→long, float→double |
| **Make nullable** | `ALTER TABLE t ALTER COLUMN col DROP NOT NULL` | Remove constraint |

### What You CAN'T Do Without Rewrite

- Narrow types (long → int)
- Change between incompatible types (string → int)
- Add NOT NULL to existing column with NULLs

---

## Catalogs: The Entry Point

### What is a Catalog?

A catalog is like a **phone book** for tables—it maps table names to their metadata locations.

```
┌─────────────────────────────────────────────────────────────┐
│                         CATALOG                              │
│                                                             │
│  "catalog.database.table_name"                              │
│            ↓                                                │
│  "s3://bucket/warehouse/database/table/metadata/v5.json"   │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

### Catalog Options

| Catalog | Best For | Notes |
|---------|----------|-------|
| **Unity Catalog** | Databricks users | Full governance, lineage, sharing |
| **Hive Metastore** | Existing Hive users | Widely supported, legacy |
| **AWS Glue** | AWS ecosystem | Native AWS integration |
| **REST Catalog** | Multi-cloud, Tabular | HTTP-based, vendor-neutral |
| **Nessie** | Git-like versioning | Branch/merge for data |
| **JDBC Catalog** | Custom setups | Store in any database |

### Why Catalog Choice Matters

```
Scenario: Multi-Engine Access

Engine A (Databricks)              Engine B (Trino)
        │                                  │
        │  "SELECT * FROM products"        │  "SELECT * FROM products"
        │                                  │
        ▼                                  ▼
┌───────────────────────────────────────────────────────────┐
│                    SHARED CATALOG                          │
│                    (e.g., AWS Glue)                        │
│                                                           │
│    products → s3://bucket/warehouse/products/metadata/    │
└───────────────────────────────────────────────────────────┘
                           │
                           ▼
                    Same data files!
                    
✅ Both engines see the same data
✅ Changes from one are visible to other
✅ No data duplication
```

**If catalogs don't match:**
```
Engine A: Uses Catalog X → metadata location A
Engine B: Uses Catalog Y → metadata location B

❌ Engines see different (or stale) data
❌ Writes may conflict or be lost
❌ Multi-engine story falls apart
```

---

## Key Takeaways

### Remember These Points

1. **Metadata is everything** — Iceberg's power comes from its layered metadata structure

2. **Snapshots are immutable** — Every change creates a new snapshot; old ones remain queryable

3. **Partitions are hidden** — Write normal queries; Iceberg handles partition pruning

4. **Schema changes are cheap** — Add, drop, rename columns without rewriting data

5. **Catalogs enable sharing** — Same catalog = same view of data across engines

### Mental Model

Think of an Iceberg table like a **Git repository for data**:

| Git Concept | Iceberg Equivalent |
|-------------|-------------------|
| Repository | Table |
| Commit | Snapshot |
| Commit history | Snapshot history |
| HEAD pointer | Current snapshot ID |
| Checkout old commit | Time travel query |
| .git folder | Metadata files |
| Working files | Data files |

---

## Next Steps

Now that you understand the concepts, proceed to:

1. **[02-databricks-setup.md](02-databricks-setup.md)** — Set up your Databricks environment
2. **[03-phase1-table-creation.md](03-phase1-table-creation.md)** — Create your first Iceberg table

---

## Further Reading

- [Apache Iceberg Specification](https://iceberg.apache.org/spec/) — The definitive technical reference
- [Iceberg Table Format Paper](https://iceberg.apache.org/papers/) — Academic background
- [Databricks Iceberg Documentation](https://docs.databricks.com/en/delta/iceberg.html) — Platform-specific guidance
