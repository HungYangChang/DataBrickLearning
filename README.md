# Databricks + Apache Iceberg Learning Project

## Slowly Changing Dimension (SCD) Pipeline for Product Catalog

This project guides you through building a production-grade data pipeline using **Apache Iceberg** on **Databricks**, teaching you the core concepts of modern lakehouse architecture.

---

## Table of Contents

1. [Overview](#overview)
2. [Core Concepts](#core-concepts)
3. [Project Architecture](#project-architecture)
4. [Environment Setup](#environment-setup)
5. [Implementation Phases](#implementation-phases)
   - [Phase 1: Ingestion and Table Creation](#phase-1-ingestion-and-table-creation)
   - [Phase 2: Merge (Upsert) Operations](#phase-2-merge-upsert-operations)
   - [Phase 3: Time Travel and Auditing](#phase-3-time-travel-and-auditing)
   - [Phase 4: Schema and Partition Evolution](#phase-4-schema-and-partition-evolution)
   - [Phase 5: Multi-Engine Access](#phase-5-multi-engine-access)
6. [Iceberg Metadata Deep Dive](#iceberg-metadata-deep-dive)
7. [Operational Considerations](#operational-considerations)
8. [Learning Resources](#learning-resources)

---

## Overview

### What We're Building

An **SCD (Slowly Changing Dimension) pipeline** that tracks a product catalog for an e-commerce company. Products change over time—prices update, descriptions change, products get discontinued. The pipeline:

- Ingests daily product snapshots
- Maintains full historical records
- Supports point-in-time queries
- Demonstrates multi-engine interoperability

### Why This Project?

This project exercises every important Iceberg feature:

| Feature | Where You'll Use It |
|---------|---------------------|
| Table Creation | Phase 1 |
| Partition Transforms | Phase 1 |
| MERGE INTO (Upsert) | Phase 2 |
| Time Travel | Phase 3 |
| Snapshot Management | Phase 3 |
| Schema Evolution | Phase 4 |
| Partition Evolution | Phase 4 |
| Multi-Engine Access | Phase 5 |

---

## Core Concepts

Before writing any code, internalize these foundational ideas.

### 1. Iceberg's Metadata Architecture

**This is the single most important concept to understand.**

An Iceberg table is NOT just a pile of Parquet files. It has a hierarchical metadata structure:

```
┌─────────────────────────────────────────────────────────────────┐
│                        CATALOG                                   │
│  (Unity Catalog, Hive Metastore, AWS Glue, REST Catalog)        │
│  → Points to current metadata.json location                      │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    METADATA FILE (JSON)                          │
│  • Table schema (with column IDs)                                │
│  • Partition spec                                                │
│  • Current snapshot ID                                           │
│  • Snapshot history                                              │
│  • Properties                                                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                        SNAPSHOT                                  │
│  • Snapshot ID                                                   │
│  • Timestamp                                                     │
│  • Operation (append, overwrite, delete)                        │
│  • Summary (added files, deleted files, etc.)                   │
│  • Points to manifest list                                       │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                     MANIFEST LIST (Avro)                         │
│  List of manifest files for this snapshot                        │
│  • Manifest file path                                            │
│  • Partition spec ID                                             │
│  • Added/deleted file counts                                     │
│  • Partition value summaries (min/max for pruning)              │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    MANIFEST FILE (Avro)                          │
│  Tracks individual data files                                    │
│  • File path                                                     │
│  • Partition tuple                                               │
│  • Record count                                                  │
│  • File size                                                     │
│  • Column-level stats (min, max, null count)                    │
└─────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                      DATA FILES (Parquet)                        │
│  The actual data, typically in Parquet format                    │
└─────────────────────────────────────────────────────────────────┘
```

**Why this matters:**
- **Atomic commits**: A new snapshot is only visible after the metadata file is atomically updated
- **Time travel**: Old snapshots remain queryable until explicitly expired
- **Query planning**: Column stats in manifests enable partition pruning and file skipping

### 2. Snapshots and Time Travel

Every write operation creates a **new snapshot**:

```
Timeline of Snapshots:
──────────────────────────────────────────────────────────────────►

Snapshot 1          Snapshot 2          Snapshot 3          Snapshot 4
(Initial Load)      (Day 2 Merge)       (Day 3 Merge)       (Schema Add)
   │                    │                    │                    │
   ▼                    ▼                    ▼                    ▼
┌──────┐            ┌──────┐            ┌──────┐            ┌──────┐
│ 1000 │            │ 1050 │            │ 1080 │            │ 1080 │
│ rows │            │ rows │            │ rows │            │ rows │
│      │            │ +50  │            │ +30  │            │ +col │
└──────┘            └──────┘            └──────┘            └──────┘

Query "AS OF" any snapshot to see historical state
```

**Key Points:**
- Old snapshots are retained (configurable retention period)
- Storage cost grows with snapshot retention
- Expiring old snapshots reclaims space but loses time travel capability

### 3. Hidden Partitioning

**Traditional (Hive-style) Partitioning:**
```
-- You must know the partition column and write queries like:
SELECT * FROM products WHERE year=2024 AND month=01 AND day=15
```

**Iceberg Hidden Partitioning:**
```
-- Just write normal predicates, Iceberg handles pruning:
SELECT * FROM products WHERE created_date = '2024-01-15'
```

Partition transforms are stored in metadata:

| Transform | Example | Resulting Partition |
|-----------|---------|---------------------|
| `year(ts)` | 2024-03-15 | 2024 |
| `month(ts)` | 2024-03-15 | 2024-03 |
| `day(ts)` | 2024-03-15 | 2024-03-15 |
| `hour(ts)` | 2024-03-15 14:30 | 2024-03-15-14 |
| `bucket(N, col)` | product_id=12345 | bucket_7 (if N=16) |
| `truncate(N, col)` | "Electronics" | "Elect" (if N=5) |

### 4. Schema Evolution

Iceberg tracks columns by **unique IDs**, not names or positions:

```
Original Schema:              After Evolution:
┌────┬────────────┬──────┐    ┌────┬──────────────────┬──────┐
│ ID │ Name       │ Type │    │ ID │ Name             │ Type │
├────┼────────────┼──────┤    ├────┼──────────────────┼──────┤
│ 1  │ product_id │ long │    │ 1  │ product_id       │ long │
│ 2  │ name       │ str  │    │ 2  │ product_name     │ str  │  ← Renamed
│ 3  │ price      │ dbl  │    │ 3  │ price            │ dbl  │
│ 4  │ category   │ str  │    │ 4  │ category         │ str  │
└────┴────────────┴──────┘    │ 5  │ sustainability   │ int  │  ← Added
                              │ -- │ (deleted col)    │ --   │
                              └────┴──────────────────┴──────┘

Old data files still readable—they just won't have column ID 5
```

**What you can do without rewriting data:**
- Add columns (new files will have them, old files return NULL)
- Drop columns (column data remains but is ignored)
- Rename columns (ID-based tracking handles this)
- Reorder columns
- Widen types (int → long, float → double)

### 5. Catalog Integration

The **catalog** is the entry point—it maps table names to metadata locations:

```
┌─────────────────────────────────────────────────────────────┐
│                         CATALOG                              │
│  "Tell me where the current metadata is for table X"        │
└─────────────────────────────────────────────────────────────┘
        │              │              │              │
        ▼              ▼              ▼              ▼
   ┌─────────┐    ┌─────────┐   ┌──────────┐   ┌─────────┐
   │ Unity   │    │  Hive   │   │   AWS    │   │  REST   │
   │ Catalog │    │Metastore│   │  Glue    │   │ Catalog │
   └─────────┘    └─────────┘   └──────────┘   └─────────┘
```

**Databricks Unity Catalog** provides:
- Centralized governance
- Fine-grained access control
- Cross-workspace table sharing
- Lineage tracking

---

## Project Architecture

### High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                 │
│  (Daily CSV/JSON product dumps, APIs, Change Data Capture)          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      LANDING / BRONZE ZONE                           │
│  Raw data as-is, append-only, full history                          │
│  Table: bronze.product_raw                                          │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       SILVER ZONE (SCD Type 2)                       │
│  Cleaned, merged, historical tracking                                │
│  Table: silver.product_history (ICEBERG)                            │
│  ┌─────────────────────────────────────────────────────────────┐    │
│  │ product_id | name | price | valid_from | valid_to | current │    │
│  └─────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                          GOLD ZONE                                   │
│  Aggregated, business-ready views                                    │
│  Table: gold.product_current (current snapshot only)                │
│  Table: gold.price_change_summary (analytics)                       │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                    ┌───────────────┼───────────────┐
                    ▼               ▼               ▼
              ┌──────────┐   ┌──────────┐   ┌──────────┐
              │Databricks│   │  Trino   │   │Snowflake │
              │  Spark   │   │          │   │          │
              └──────────┘   └──────────┘   └──────────┘
```

### Storage Layout

```
s3://your-bucket/
└── iceberg/
    └── warehouse/
        ├── bronze/
        │   └── product_raw/
        │       ├── metadata/
        │       │   ├── v1.metadata.json
        │       │   ├── v2.metadata.json
        │       │   └── ...
        │       └── data/
        │           ├── ingestion_date=2024-01-01/
        │           ├── ingestion_date=2024-01-02/
        │           └── ...
        ├── silver/
        │   └── product_history/
        │       ├── metadata/
        │       └── data/
        │           ├── category=Electronics/
        │           ├── category=Clothing/
        │           └── ...
        └── gold/
            └── product_current/
                ├── metadata/
                └── data/
```

---

## Environment Setup

### Prerequisites Checklist

- [ ] Databricks account (Community Edition works for learning)
- [ ] Cloud storage bucket (S3, ADLS, or GCS)
- [ ] Basic understanding of SQL and Spark concepts

### Step 1: Databricks Workspace Setup

1. **Create a Databricks Workspace**
   - Community Edition: https://community.cloud.databricks.com/
   - For production: Use your cloud provider's Databricks integration

2. **Configure Unity Catalog** (if available)
   - Unity Catalog provides the best Iceberg integration
   - Creates a metastore to track all your tables

3. **Create a Compute Cluster**
   - For learning: Single node, smallest instance
   - Runtime: Databricks Runtime 13.3 LTS or later (includes Iceberg support)
   - Enable "Photon" for better performance (optional)

### Step 2: Storage Configuration

1. **Create Cloud Storage Bucket**
   ```
   Bucket name suggestion: your-org-iceberg-learning
   
   Folder structure to create:
   /iceberg/warehouse/bronze/
   /iceberg/warehouse/silver/
   /iceberg/warehouse/gold/
   ```

2. **Configure Storage Credentials**
   - In Databricks: Admin Console → Storage Credentials
   - Create an external location pointing to your bucket

### Step 3: Catalog Configuration

**Option A: Unity Catalog (Recommended)**
- Tables are automatically registered
- Full governance and lineage
- Use 3-level namespace: `catalog.schema.table`

**Option B: Hive Metastore**
- Legacy approach, still widely used
- Use 2-level namespace: `database.table`

**Option C: AWS Glue Catalog**
- Good for cross-service AWS integration
- Configure in Spark session settings

---

## Implementation Phases

### Phase 1: Ingestion and Table Creation

#### Objective
Create your first Iceberg table and land initial product data.

#### Key Decisions

**Schema Design:**
```
product_history table:
├── product_id      (BIGINT)       - Primary identifier
├── product_name    (STRING)       - Product display name
├── category        (STRING)       - Product category
├── price           (DECIMAL)      - Current price
├── description     (STRING)       - Product description
├── is_active       (BOOLEAN)      - Active/discontinued flag
├── valid_from      (TIMESTAMP)    - When this version became active
├── valid_to        (TIMESTAMP)    - When this version was superseded (NULL if current)
├── is_current      (BOOLEAN)      - Quick filter for current records
├── ingestion_ts    (TIMESTAMP)    - When we loaded this record
└── source_file     (STRING)       - Lineage tracking
```

**Partition Strategy:**
```
Considerations:
- Query patterns: Most queries filter by category or time range
- Cardinality: ~50 categories, daily updates
- File size: Target 128MB-1GB per file

Recommended approach:
- Partition by: bucket(16, product_id), month(valid_from)
- This balances query performance with write efficiency
```

#### Implementation Steps

1. **Create the catalog/schema structure**
   - Define your catalog (if using Unity Catalog)
   - Create bronze, silver, gold schemas

2. **Create the Iceberg table**
   - Use `CREATE TABLE ... USING ICEBERG`
   - Specify partition transforms
   - Set table properties (write format, compression)

3. **Generate sample data**
   - Create a DataFrame with ~1000 sample products
   - Include varied categories and price ranges

4. **Write initial load**
   - Use `df.writeTo("table").append()`
   - Verify data landed correctly

5. **Inspect the metadata**
   - Navigate to your storage bucket
   - Examine the metadata.json file
   - Understand the manifest structure

#### Validation Checklist
- [ ] Table appears in Databricks catalog
- [ ] Can query the table successfully
- [ ] Metadata files exist in storage
- [ ] Correct number of data files created

---

### Phase 2: Merge (Upsert) Operations

#### Objective
Implement the daily merge logic that forms the core of SCD Type 2.

#### Understanding MERGE INTO

```
MERGE INTO target
USING source
ON match_condition
WHEN MATCHED AND condition THEN UPDATE SET ...
WHEN MATCHED AND condition THEN DELETE
WHEN NOT MATCHED THEN INSERT ...
```

**For SCD Type 2, the logic is:**
1. **New products**: INSERT with `valid_from = today`, `is_current = true`
2. **Changed products**: 
   - UPDATE existing record: `valid_to = today`, `is_current = false`
   - INSERT new record: `valid_from = today`, `is_current = true`
3. **Unchanged products**: No action needed
4. **Deleted products**: UPDATE `is_active = false`, `valid_to = today`

#### Copy-on-Write vs Merge-on-Read

Iceberg supports two strategies:

| Aspect | Copy-on-Write | Merge-on-Read |
|--------|---------------|---------------|
| Write performance | Slower (rewrites files) | Faster (writes delete files) |
| Read performance | Faster (no merge needed) | Slower (must apply deletes) |
| Best for | Read-heavy workloads | Write-heavy workloads |
| Storage | Uses more space during writes | Uses more metadata files |

**Configuration:**
```
Table property: write.delete.mode = 'copy-on-write' | 'merge-on-read'
Table property: write.update.mode = 'copy-on-write' | 'merge-on-read'
```

#### Implementation Steps

1. **Generate "Day 2" source data**
   - Include some new products
   - Modify prices/descriptions of existing products
   - Mark some products as discontinued

2. **Implement the MERGE statement**
   - Match on `product_id` and `is_current = true`
   - Handle all three cases (new, changed, deleted)

3. **Execute and observe**
   - Run the merge
   - Check snapshot count increased
   - Verify history is preserved

4. **Repeat for Days 3, 4, 5**
   - Build up snapshot history
   - Practice the merge workflow

#### Validation Checklist
- [ ] Merge completes successfully
- [ ] New snapshot created for each merge
- [ ] Historical records preserved (valid_to populated)
- [ ] Current records have is_current = true
- [ ] Query for specific product shows full history

---

### Phase 3: Time Travel and Auditing

#### Objective
Query historical states and build an audit trail.

#### Time Travel Syntax

```sql
-- Query by timestamp
SELECT * FROM catalog.schema.product_history
TIMESTAMP AS OF '2024-01-15 10:00:00'

-- Query by snapshot ID
SELECT * FROM catalog.schema.product_history
VERSION AS OF 1234567890

-- Query by snapshot relative to current
SELECT * FROM catalog.schema.product_history
VERSION AS OF <snapshot_id>
```

#### Snapshot Management

```sql
-- List all snapshots
SELECT * FROM catalog.schema.product_history.snapshots

-- List history (operations)
SELECT * FROM catalog.schema.product_history.history

-- Rollback to previous snapshot
CALL catalog.system.rollback_to_snapshot('schema.product_history', <snapshot_id>)

-- Expire old snapshots (reclaim storage)
CALL catalog.system.expire_snapshots('schema.product_history', TIMESTAMP '2024-01-01')
```

#### Building an Audit View

Create a view that shows what changed between snapshots:

**Approach 1: Compare specific snapshots**
```
Join table@snapshot_1 with table@snapshot_2 on product_id
Identify: added (null left), removed (null right), changed (values differ)
```

**Approach 2: Incremental changes**
```
Use Iceberg's .changes() API to get only modified rows
More efficient than full snapshot comparison
```

#### Implementation Steps

1. **List your snapshots**
   - Query the snapshots metadata table
   - Note timestamps and snapshot IDs

2. **Query historical states**
   - Query "AS OF" each snapshot
   - Verify data matches expected state

3. **Build change detection query**
   - Compare snapshot N-1 to snapshot N
   - Output: product_id, change_type, old_value, new_value

4. **Create audit summary table**
   - Materialize the changes for reporting
   - Include: snapshot_id, change_count, timestamp

5. **Practice snapshot expiration**
   - Expire a single old snapshot
   - Verify time travel to that snapshot fails
   - Verify storage was reclaimed

#### Validation Checklist
- [ ] Can query any previous snapshot
- [ ] Change detection query works correctly
- [ ] Audit view shows meaningful diffs
- [ ] Snapshot expiration runs successfully
- [ ] Understand storage cost vs time travel tradeoff

---

### Phase 4: Schema and Partition Evolution

#### Objective
Evolve your table without rewriting existing data.

#### Schema Evolution Operations

```sql
-- Add a column
ALTER TABLE product_history ADD COLUMN sustainability_rating INT

-- Rename a column
ALTER TABLE product_history RENAME COLUMN price TO unit_price

-- Drop a column (metadata only, data remains)
ALTER TABLE product_history DROP COLUMN legacy_field

-- Change column type (widening only)
ALTER TABLE product_history ALTER COLUMN product_id TYPE BIGINT
```

#### Partition Evolution

```sql
-- View current partition spec
SELECT * FROM catalog.schema.product_history.partitions

-- Add new partition field
ALTER TABLE product_history ADD PARTITION FIELD day(valid_from)

-- Drop partition field (new writes won't use it)
ALTER TABLE product_history DROP PARTITION FIELD month(valid_from)
```

**Critical understanding:**
- Old data files keep their old partition layout
- New data files use the new partition spec
- Queries work seamlessly across both
- Iceberg tracks partition spec history in metadata

#### Implementation Steps

1. **Add the sustainability_rating column**
   - Run ALTER TABLE ADD COLUMN
   - Verify old data returns NULL for new column
   - Insert new data with the column populated

2. **Change partition granularity**
   - Current: `month(valid_from)`
   - New: `day(valid_from)`
   - Run ALTER TABLE ADD/DROP PARTITION FIELD

3. **Verify mixed partition layouts**
   - Check data files in storage
   - Old files: monthly partitions
   - New files: daily partitions
   - Query works across both

4. **Inspect metadata changes**
   - Compare metadata.json before and after
   - Note the partition-spec-id changes

#### Validation Checklist
- [ ] Schema changes don't require data rewrite
- [ ] Old data accessible with new schema
- [ ] Partition evolution doesn't break existing data
- [ ] Queries span old and new partition layouts
- [ ] Metadata reflects evolution history

---

### Phase 5: Multi-Engine Access

#### Objective
Prove Iceberg's engine-agnostic promise by reading your data from another engine.

#### Engine Options

**Option A: Local Spark with Iceberg**
- Download Spark locally
- Configure Iceberg catalog pointing to your storage
- Query the same tables you created in Databricks

**Option B: Trino (formerly PrestoSQL)**
- Run Trino locally or use a managed service
- Configure Iceberg connector
- Point to the same catalog

**Option C: Snowflake Iceberg Tables**
- If you have Snowflake access
- Create an external Iceberg table
- Point to your existing data

**Option D: AWS Athena**
- Serverless option
- Configure Glue catalog integration
- Query Iceberg tables directly

#### Catalog Configuration

The key is ensuring both engines point to the same catalog:

```
Engine A (Databricks):
  catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
  warehouse = s3://bucket/iceberg/warehouse
  
Engine B (Local Spark):
  catalog-impl = org.apache.iceberg.aws.glue.GlueCatalog
  warehouse = s3://bucket/iceberg/warehouse  ← Same location!
```

#### Implementation Steps

1. **Choose your second engine**
   - Local Spark is easiest to set up
   - Trino if you want SQL-focused experience

2. **Configure storage access**
   - Ensure credentials work from new environment
   - Test basic file listing first

3. **Configure the Iceberg catalog**
   - Match the catalog type used in Databricks
   - Point to the same warehouse location

4. **Query the table**
   - Run simple SELECT
   - Verify row counts match
   - Test time travel from new engine

5. **Test write from second engine** (optional)
   - Write new data from Engine B
   - Verify Engine A sees the changes

#### Validation Checklist
- [ ] Second engine successfully connects
- [ ] Can list tables in the catalog
- [ ] Query results match Databricks
- [ ] Time travel works from second engine
- [ ] Demonstrated true engine interoperability

---

## Iceberg Metadata Deep Dive

### File Structure Walkthrough

After running your pipeline, explore the actual files:

```
product_history/
├── metadata/
│   ├── v1.metadata.json         ← Initial table creation
│   ├── v2.metadata.json         ← After first merge
│   ├── v3.metadata.json         ← After second merge
│   ├── snap-1234567890-1-uuid.avro   ← Manifest list for snapshot
│   ├── uuid-m0.avro             ← Manifest file
│   └── version-hint.text        ← Points to current metadata
└── data/
    ├── category=Electronics/
    │   ├── 00000-0-uuid.parquet
    │   └── 00001-0-uuid.parquet
    └── category=Clothing/
        └── 00000-0-uuid.parquet
```

### Reading Metadata Files

**metadata.json example (simplified):**
```json
{
  "format-version": 2,
  "table-uuid": "uuid-here",
  "location": "s3://bucket/iceberg/warehouse/silver/product_history",
  "schemas": [
    {
      "schema-id": 0,
      "fields": [
        {"id": 1, "name": "product_id", "type": "long"},
        {"id": 2, "name": "product_name", "type": "string"}
      ]
    }
  ],
  "current-schema-id": 0,
  "partition-specs": [
    {
      "spec-id": 0,
      "fields": [
        {"source-id": 1, "field-id": 1000, "name": "product_id_bucket", "transform": "bucket[16]"}
      ]
    }
  ],
  "current-snapshot-id": 1234567890,
  "snapshots": [
    {
      "snapshot-id": 1234567890,
      "timestamp-ms": 1705312800000,
      "manifest-list": "s3://bucket/.../snap-1234567890-1-uuid.avro"
    }
  ]
}
```

---

## Operational Considerations

### Small File Problem

**Symptoms:**
- Thousands of tiny files (<10MB each)
- Slow query performance
- High metadata overhead

**Causes:**
- Streaming ingestion
- Frequent small batch writes
- High-cardinality partitions

**Solutions:**

1. **Manual Compaction**
   ```sql
   -- Databricks OPTIMIZE
   OPTIMIZE catalog.schema.product_history
   
   -- Iceberg rewrite_data_files procedure
   CALL catalog.system.rewrite_data_files('schema.product_history')
   ```

2. **Automatic Compaction**
   - Configure write.target-file-size-bytes
   - Enable auto-compaction in table properties

3. **Write Batching**
   - Accumulate data before writing
   - Use Spark's coalesce() before write

### Snapshot Management

**Retention strategy:**
```
Production recommendation:
- Keep 7-30 days of snapshots for time travel
- Archive older snapshots to cold storage if needed
- Run expiration on a schedule (daily or weekly)
```

**Storage cost formula:**
```
Total Storage = (Active Data) + (Orphaned Files from Old Snapshots)

After expiration:
- Orphaned data files are deleted
- Only current snapshot's files remain
- Storage cost drops significantly
```

### Cost Awareness

| Operation | Cost Driver | Optimization |
|-----------|-------------|--------------|
| MERGE | Compute (file rewrites) | Batch updates, merge-on-read |
| Time Travel | Storage (snapshot retention) | Aggressive expiration |
| Compaction | Compute + temporary storage | Off-peak scheduling |
| Queries | Compute (scanned data) | Partition pruning, column stats |

---

## Learning Resources

### Official Documentation

1. **Apache Iceberg Spec** (Essential reading)
   - https://iceberg.apache.org/spec/
   - Focus on: Table Spec, Partition Spec, Schema Evolution

2. **Databricks Iceberg Documentation**
   - https://docs.databricks.com/en/delta/iceberg.html
   - Unity Catalog integration details

3. **Iceberg Quickstart**
   - https://iceberg.apache.org/docs/latest/getting-started/

### Recommended Reading Order

1. Iceberg Table Spec (understand the metadata model)
2. Databricks Unity Catalog overview
3. Iceberg Spark integration guide
4. Time Travel and snapshot management
5. Schema and partition evolution

### Community Resources

- Apache Iceberg GitHub: https://github.com/apache/iceberg
- Iceberg Slack community
- Tabular blog (Iceberg creators): https://tabular.io/blog/

---

## Project Folder Structure

```
DataBrickLearning/
├── README.md                          ← You are here
├── docs/
│   ├── 01-iceberg-concepts.md         ← Detailed concept explanations
│   ├── 02-databricks-setup.md         ← Step-by-step setup guide
│   └── 03-troubleshooting.md          ← Common issues and solutions
├── notebooks/
│   ├── 01-setup-and-initial-load.py   ← Phase 1 notebook
│   ├── 02-merge-operations.py         ← Phase 2 notebook
│   ├── 03-time-travel-audit.py        ← Phase 3 notebook
│   ├── 04-schema-evolution.py         ← Phase 4 notebook
│   └── 05-multi-engine-access.py      ← Phase 5 notebook
├── sample-data/
│   ├── products_day1.csv              ← Initial load data
│   ├── products_day2.csv              ← Day 2 changes
│   ├── products_day3.csv              ← Day 3 changes
│   └── data-generator.py              ← Script to generate test data
└── config/
    ├── spark-iceberg.conf             ← Local Spark config
    └── trino-iceberg.properties       ← Trino connector config
```

---

## Next Steps

1. **Read the Iceberg Table Spec** - This is non-negotiable. Understanding the metadata model is foundational.

2. **Set up your Databricks environment** - Get a workspace running, even Community Edition.

3. **Start Phase 1** - Create your first table and inspect the metadata files.

4. **Document as you go** - Update this README with your learnings and any issues you encounter.

Good luck! The Iceberg metadata architecture will feel very familiar given your distributed systems background—it shares conceptual DNA with version control and transactional protocols.
