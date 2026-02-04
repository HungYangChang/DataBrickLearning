# Phase 5: Multi-Engine Access

> **Objective:** Prove Iceberg's engine-agnostic promise  
> **Time Required:** 2-3 hours  
> **Prerequisites:** Completed [Phase 4](06-phase4-evolution.md), access to cloud storage

---

## What You'll Learn

By the end of this phase, you will:

- ✅ Understand why multi-engine access matters
- ✅ Configure a second engine to read Iceberg tables
- ✅ Verify data consistency across engines
- ✅ Test time travel from different engines
- ✅ Understand catalog configuration requirements

---

## Why Multi-Engine Access Matters

### The Vendor Lock-in Problem

```
TRADITIONAL DATA PLATFORMS:
───────────────────────────

┌─────────────────────────────────────────────────────────────┐
│                     VENDOR A                                 │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────────┐   │
│  │   Compute   │────▶│ Proprietary │────▶│   Storage   │   │
│  │   Engine    │     │   Format    │     │             │   │
│  └─────────────┘     └─────────────┘     └─────────────┘   │
│                                                             │
│  Want to use Vendor B's engine?                            │
│  → Export data (slow, expensive)                           │
│  → Convert format (data quality risks)                     │
│  → Lose history and metadata                               │
└─────────────────────────────────────────────────────────────┘
```

### The Iceberg Solution

```
ICEBERG ARCHITECTURE:
─────────────────────

┌─────────────────────────────────────────────────────────────┐
│                                                             │
│     ┌───────────┐  ┌───────────┐  ┌───────────┐           │
│     │Databricks │  │   Trino   │  │ Snowflake │           │
│     │   Spark   │  │           │  │           │           │
│     └─────┬─────┘  └─────┬─────┘  └─────┬─────┘           │
│           │              │              │                  │
│           └──────────────┼──────────────┘                  │
│                          │                                 │
│                          ▼                                 │
│                  ┌───────────────┐                         │
│                  │    CATALOG    │                         │
│                  │ (Shared View) │                         │
│                  └───────┬───────┘                         │
│                          │                                 │
│                          ▼                                 │
│                  ┌───────────────┐                         │
│                  │   ICEBERG     │                         │
│                  │   METADATA    │                         │
│                  └───────┬───────┘                         │
│                          │                                 │
│                          ▼                                 │
│                  ┌───────────────┐                         │
│                  │  DATA FILES   │                         │
│                  │  (Parquet)    │                         │
│                  └───────────────┘                         │
│                                                             │
│  All engines see the same data, same history!              │
└─────────────────────────────────────────────────────────────┘
```

---

## Step 1: Understand Your Options

### Engine Options for Multi-Engine Demo

| Engine | Setup Complexity | Best For |
|--------|------------------|----------|
| **Local Spark** | Medium | Learning, full Iceberg API |
| **Trino** | Medium | SQL-focused, fast queries |
| **AWS Athena** | Low | Serverless, AWS users |
| **Snowflake** | Low | Enterprise users |
| **DuckDB** | Very Low | Local testing, small data |

### Requirements by Engine

```
COMMON REQUIREMENTS:
────────────────────
✓ Access to same cloud storage (S3, ADLS, GCS)
✓ Access to same catalog (or compatible catalog)
✓ Network connectivity to storage
✓ Proper authentication configured

ENGINE-SPECIFIC:
────────────────
Spark:    Iceberg runtime JAR, catalog configuration
Trino:    Iceberg connector, catalog properties file
Athena:   AWS Glue catalog integration
Snowflake: External volume + Iceberg table definition
DuckDB:   iceberg extension, direct metadata access
```

---

## Step 2: Prerequisites Check

### Step 2.1: Verify Your Storage Location

```sql
-- Cell 1: Get your table's storage location
-- =========================================

-- In Databricks, get the table location
DESCRIBE EXTENDED iceberg_learning.product_history;

-- Note the "Location" field, something like:
-- s3://your-bucket/iceberg/warehouse/iceberg_learning.db/product_history
-- or
-- abfss://container@storage.dfs.core.windows.net/iceberg/warehouse/...
-- or  
-- dbfs:/user/hive/warehouse/iceberg_learning.db/product_history
```

**Important:** If you're using Community Edition with DBFS, multi-engine access is limited. You'll need cloud storage (S3, ADLS, or GCS) for true multi-engine access.

### Step 2.2: Verify Catalog Type

```sql
-- Cell 2: Check catalog configuration
-- ===================================

-- Show current catalog
SELECT current_catalog();

-- List available catalogs
SHOW CATALOGS;
```

---

## Step 3: Option A — Local Spark + Iceberg

### Step 3.1: Install Spark Locally

**macOS (Homebrew):**
```bash
# Install Java (if needed)
brew install openjdk@11

# Install Spark
brew install apache-spark

# Verify installation
spark-shell --version
```

**Linux:**
```bash
# Download Spark
wget https://downloads.apache.org/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz
tar xzf spark-3.5.0-bin-hadoop3.tgz
export SPARK_HOME=$(pwd)/spark-3.5.0-bin-hadoop3
export PATH=$SPARK_HOME/bin:$PATH
```

**Windows:**
```
Download from: https://spark.apache.org/downloads.html
Extract and set SPARK_HOME environment variable
```

### Step 3.2: Download Iceberg Dependencies

Create a directory for your project and download required JARs:

```bash
# Create project directory
mkdir ~/iceberg-local-demo
cd ~/iceberg-local-demo

# Download Iceberg runtime (match your Spark version)
# For Spark 3.5:
curl -O https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar

# For AWS S3 access:
curl -O https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar
curl -O https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar
```

### Step 3.3: Create Spark Configuration

Create `spark-iceberg.conf`:

```properties
# Iceberg Configuration for Local Spark
# =====================================

# Iceberg extensions
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions

# Catalog configuration (using AWS Glue as example)
spark.sql.catalog.glue_catalog=org.apache.iceberg.spark.SparkCatalog
spark.sql.catalog.glue_catalog.warehouse=s3://your-bucket/iceberg/warehouse
spark.sql.catalog.glue_catalog.catalog-impl=org.apache.iceberg.aws.glue.GlueCatalog
spark.sql.catalog.glue_catalog.io-impl=org.apache.iceberg.aws.s3.S3FileIO

# AWS credentials (or use instance profile/environment variables)
spark.hadoop.fs.s3a.access.key=YOUR_ACCESS_KEY
spark.hadoop.fs.s3a.secret.key=YOUR_SECRET_KEY
spark.hadoop.fs.s3a.endpoint=s3.amazonaws.com

# For REST Catalog instead:
# spark.sql.catalog.rest_catalog=org.apache.iceberg.spark.SparkCatalog
# spark.sql.catalog.rest_catalog.type=rest
# spark.sql.catalog.rest_catalog.uri=http://localhost:8181
```

### Step 3.4: Start Spark Shell with Iceberg

```bash
# Start spark-shell with Iceberg
spark-shell \
  --packages org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.4.3 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://your-bucket/iceberg/warehouse
```

### Step 3.5: Query Your Table from Local Spark

```scala
// In spark-shell (Scala)
// ======================

// List tables in your catalog
spark.sql("SHOW TABLES IN glue_catalog.iceberg_learning").show()

// Query the product_history table
spark.sql("""
  SELECT 
    product_id, 
    product_name, 
    price, 
    valid_from 
  FROM glue_catalog.iceberg_learning.product_history 
  WHERE is_current = true 
  LIMIT 10
""").show()

// Test time travel
spark.sql("""
  SELECT COUNT(*) as record_count
  FROM glue_catalog.iceberg_learning.product_history
  TIMESTAMP AS OF '2024-01-10 00:00:00'
""").show()

// View snapshots
spark.sql("""
  SELECT snapshot_id, committed_at, operation
  FROM glue_catalog.iceberg_learning.product_history.snapshots
""").show()
```

---

## Step 4: Option B — Trino

### Step 4.1: Install Trino Locally

**Using Docker (Recommended):**
```bash
# Pull Trino image
docker pull trinodb/trino:latest

# Create config directory
mkdir -p ~/trino-iceberg/etc/catalog
```

### Step 4.2: Configure Iceberg Connector

Create `~/trino-iceberg/etc/catalog/iceberg.properties`:

```properties
# Iceberg Connector Configuration
# ================================

connector.name=iceberg

# For AWS Glue Catalog:
iceberg.catalog.type=glue
iceberg.glue.region=us-east-1

# For Hive Metastore:
# iceberg.catalog.type=hive
# hive.metastore.uri=thrift://localhost:9083

# For REST Catalog:
# iceberg.catalog.type=rest
# iceberg.rest-catalog.uri=http://localhost:8181

# S3 Configuration
hive.s3.aws-access-key=YOUR_ACCESS_KEY
hive.s3.aws-secret-key=YOUR_SECRET_KEY
hive.s3.endpoint=s3.amazonaws.com
```

### Step 4.3: Start Trino

```bash
# Start Trino with Docker
docker run -d \
  --name trino-iceberg \
  -p 8080:8080 \
  -v ~/trino-iceberg/etc:/etc/trino \
  trinodb/trino:latest

# Check logs
docker logs trino-iceberg

# Connect with Trino CLI
docker exec -it trino-iceberg trino
```

### Step 4.4: Query from Trino

```sql
-- In Trino CLI
-- ============

-- List schemas
SHOW SCHEMAS FROM iceberg;

-- List tables
SHOW TABLES FROM iceberg.iceberg_learning;

-- Query the table
SELECT 
    product_id,
    product_name,
    price,
    valid_from
FROM iceberg.iceberg_learning.product_history
WHERE is_current = true
LIMIT 10;

-- Time travel
SELECT COUNT(*) as record_count
FROM iceberg.iceberg_learning.product_history
FOR VERSION AS OF 1234567890;  -- Replace with snapshot ID

-- View snapshots
SELECT * FROM iceberg.iceberg_learning."product_history$snapshots";
```

---

## Step 5: Option C — AWS Athena

### Step 5.1: Prerequisites

- AWS Glue Catalog must be your Iceberg catalog
- Table must be in S3

### Step 5.2: Configure Athena

1. Open AWS Athena Console
2. Set up a query result location in S3
3. Select your database from Glue Catalog

### Step 5.3: Query from Athena

```sql
-- In Athena Console
-- =================

-- Set database
USE iceberg_learning;

-- Query table
SELECT 
    product_id,
    product_name,
    price,
    valid_from
FROM product_history
WHERE is_current = true
LIMIT 10;

-- Time travel (Athena syntax)
SELECT COUNT(*) as record_count
FROM product_history
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-10 00:00:00';

-- View table metadata
DESCRIBE EXTENDED product_history;
```

---

## Step 6: Option D — DuckDB (Simplest Local Option)

### Step 6.1: Install DuckDB

```bash
# macOS
brew install duckdb

# Linux (using pip)
pip install duckdb

# Or download from https://duckdb.org/
```

### Step 6.2: Configure Iceberg Extension

```sql
-- In DuckDB CLI
-- =============

-- Install and load Iceberg extension
INSTALL iceberg;
LOAD iceberg;

-- Install and load httpfs for S3 access
INSTALL httpfs;
LOAD httpfs;

-- Configure S3 credentials
SET s3_region='us-east-1';
SET s3_access_key_id='YOUR_ACCESS_KEY';
SET s3_secret_access_key='YOUR_SECRET_KEY';
```

### Step 6.3: Query Iceberg Table

```sql
-- Query using direct metadata path
-- ================================

-- DuckDB can read Iceberg tables directly from metadata
SELECT * 
FROM iceberg_scan('s3://your-bucket/iceberg/warehouse/iceberg_learning.db/product_history')
LIMIT 10;

-- With time travel (by snapshot)
SELECT * 
FROM iceberg_scan(
    's3://your-bucket/iceberg/warehouse/iceberg_learning.db/product_history',
    version := '1234567890'  -- snapshot ID
)
LIMIT 10;
```

---

## Step 7: Verify Cross-Engine Consistency

### Step 7.1: Run Comparison Queries

Run the same queries from both Databricks and your second engine:

**Query 1: Record Count**
```sql
SELECT COUNT(*) as total_records
FROM product_history;
```

**Query 2: Category Summary**
```sql
SELECT 
    category,
    COUNT(*) as product_count,
    ROUND(AVG(price), 2) as avg_price
FROM product_history
WHERE is_current = true
GROUP BY category
ORDER BY product_count DESC;
```

**Query 3: Time Travel**
```sql
SELECT COUNT(*) as historical_count
FROM product_history
-- Use appropriate time travel syntax for each engine
TIMESTAMP AS OF '2024-01-10 00:00:00';
```

### Step 7.2: Document Results

Create a comparison table:

| Query | Databricks Result | Second Engine Result | Match? |
|-------|-------------------|----------------------|--------|
| Total Records | 1,170 | 1,170 | ✅ |
| Electronics Count | 200 | 200 | ✅ |
| Historical Count | 1,000 | 1,000 | ✅ |

---

## Step 8: Test Write from Second Engine (Advanced)

### Step 8.1: Insert from Second Engine

**From Local Spark:**
```scala
// Insert a test record
spark.sql("""
  INSERT INTO glue_catalog.iceberg_learning.product_history
  VALUES (
    9999, 'Cross-Engine Test Product', 'Electronics', 'Test', 'TestBrand',
    99.99, 49.99, 'Inserted from local Spark', true,
    current_timestamp(), null, true,
    current_timestamp(), 'local_spark_test.csv', 'testhash',
    null, null
  )
""")
```

**From Trino:**
```sql
INSERT INTO iceberg.iceberg_learning.product_history
VALUES (
    9998, 'Trino Test Product', 'Electronics', 'Test', 'TestBrand',
    88.88, 44.44, 'Inserted from Trino', true,
    current_timestamp, null, true,
    current_timestamp, 'trino_test.csv', 'trinohash',
    null, null
);
```

### Step 8.2: Verify in Databricks

```sql
-- Back in Databricks
-- ==================

-- Check for cross-engine inserts
SELECT 
    product_id,
    product_name,
    source_file
FROM iceberg_learning.product_history
WHERE product_id >= 9998
ORDER BY product_id;

-- Verify new snapshots created
SELECT 
    snapshot_id,
    committed_at,
    summary['added-records'] as added
FROM iceberg_learning.product_history.snapshots
ORDER BY committed_at DESC
LIMIT 5;
```

---

## Step 9: Troubleshooting Multi-Engine Access

### Common Issues

**Issue 1: "Table not found"**
```
Cause: Catalog configuration mismatch
Solution: Verify both engines point to same catalog location

Check in Databricks:
  - Table location: DESCRIBE EXTENDED product_history
  
Check in second engine:
  - Catalog configuration matches
  - Warehouse path is identical
```

**Issue 2: "Access denied to S3/ADLS"**
```
Cause: Credentials or permissions issue
Solution: 
  - Verify credentials are correct
  - Check IAM policies/RBAC permissions
  - Test with: aws s3 ls s3://your-bucket/
```

**Issue 3: "Schema mismatch"**
```
Cause: Stale metadata cache
Solution:
  - Refresh metadata: REFRESH TABLE product_history
  - Clear engine cache
  - Verify catalog is showing latest snapshot
```

**Issue 4: "Snapshot not found"**
```
Cause: Snapshot may have been expired
Solution:
  - List available snapshots
  - Use a valid snapshot ID
  - Check retention settings
```

---

## Validation Checklist

Congratulations! You've completed the learning journey. Verify:

- [ ] Second engine connected successfully
- [ ] Can list tables from second engine
- [ ] Query results match between engines
- [ ] Time travel works from second engine
- [ ] (Advanced) Write from second engine succeeded
- [ ] (Advanced) Databricks sees cross-engine writes

---

## Summary: What You've Accomplished

### The Complete Picture

```
┌─────────────────────────────────────────────────────────────────────┐
│                    YOUR ICEBERG LAKEHOUSE                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  PHASE 1: TABLE CREATION ✓                                         │
│    • Designed SCD Type 2 schema                                    │
│    • Implemented bucket + month partitioning                       │
│    • Loaded 1,000 products                                         │
│                                                                     │
│  PHASE 2: MERGE OPERATIONS ✓                                       │
│    • Implemented daily merge logic                                 │
│    • Handled inserts, updates, deletes                            │
│    • Built up snapshot history                                     │
│                                                                     │
│  PHASE 3: TIME TRAVEL ✓                                            │
│    • Queried historical states                                     │
│    • Built audit reports                                           │
│    • Managed snapshot retention                                    │
│                                                                     │
│  PHASE 4: EVOLUTION ✓                                              │
│    • Added columns without rewrite                                 │
│    • Evolved partition strategy                                    │
│    • Saw old/new schemas coexist                                   │
│                                                                     │
│  PHASE 5: MULTI-ENGINE ✓                                           │
│    • Configured second engine                                      │
│    • Verified data consistency                                     │
│    • Proved vendor independence                                    │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

### Key Takeaways

1. **Iceberg is metadata magic** — The layered metadata structure enables all features.

2. **Snapshots are your safety net** — Every change is tracked and reversible.

3. **Schema/partition evolution is painless** — No more costly migrations.

4. **Catalogs are the key to sharing** — Same catalog = same view everywhere.

5. **Engine independence is real** — Your data outlives any single platform.

---

## Next Steps for Production

### Recommended Production Practices

1. **Automate Maintenance**
   - Schedule snapshot expiration
   - Run compaction regularly
   - Monitor file sizes

2. **Implement Governance**
   - Use Unity Catalog or similar
   - Set up access controls
   - Enable audit logging

3. **Optimize Performance**
   - Tune partition strategy for query patterns
   - Use sort orders for frequently filtered columns
   - Consider merge-on-read for write-heavy workloads

4. **Plan for Scale**
   - Design partitions for data growth
   - Implement incremental processing
   - Monitor metadata size

---

## Additional Resources

### Official Documentation
- [Apache Iceberg](https://iceberg.apache.org/docs/latest/)
- [Databricks Iceberg](https://docs.databricks.com/en/delta/iceberg.html)
- [Trino Iceberg](https://trino.io/docs/current/connector/iceberg.html)

### Community
- Apache Iceberg Slack
- Databricks Community Forums
- Tabular Blog (Iceberg creators)

### Books & Courses
- "Apache Iceberg: The Definitive Guide" (O'Reilly)
- Databricks Academy courses
- YouTube: Iceberg deep dives by Tabular

---

## Congratulations! 🎉

You've completed the Databricks + Apache Iceberg learning project. You now understand:

- How Iceberg's metadata architecture works
- How to build production-grade SCD pipelines
- How to leverage time travel and auditing
- How to evolve tables without downtime
- How to achieve true multi-engine interoperability

You're ready to apply these concepts to real-world data engineering challenges!
