# Troubleshooting Guide

> A comprehensive guide to solving common issues with Databricks and Apache Iceberg

---

## Table of Contents

1. [Databricks Issues](#databricks-issues)
2. [Iceberg Table Issues](#iceberg-table-issues)
3. [Storage and Permissions](#storage-and-permissions)
4. [Performance Issues](#performance-issues)
5. [Multi-Engine Issues](#multi-engine-issues)
6. [Error Reference](#error-reference)

---

## Databricks Issues

### Cluster Won't Start

**Symptoms:**
- Cluster stays in "Pending" state for >10 minutes
- Status shows "Error" with message

**Solutions:**

| Cause | Solution |
|-------|----------|
| Instance quota exceeded | Request quota increase in cloud provider |
| Invalid instance type | Choose different instance type in cluster config |
| VPC/Network issues | Check subnet has internet access |
| Insufficient permissions | Verify IAM role/service principal permissions |

**Diagnostic Steps:**
```
1. Go to Cluster → Event Log
2. Look for error messages
3. Check "Driver Logs" for details
```

### Notebook Won't Attach to Cluster

**Symptoms:**
- "Detached" status won't change
- Cells won't execute

**Solutions:**
1. Verify cluster is in "Running" state
2. Refresh the browser page
3. Try: Detach → Reattach
4. Restart the cluster
5. Create a new notebook (rare but sometimes needed)

### "Command Timed Out" Error

**Symptoms:**
- Long-running queries fail with timeout

**Solutions:**
```python
# Increase timeout in notebook settings
spark.conf.set("spark.sql.broadcastTimeout", "600")  # 10 minutes
spark.conf.set("spark.network.timeout", "600s")
```

Or break query into smaller operations.

---

## Iceberg Table Issues

### "Table Not Found"

**Symptoms:**
```
AnalysisException: Table or view not found: iceberg_learning.product_history
```

**Solutions:**

1. **Check database exists:**
   ```sql
   SHOW DATABASES;
   USE iceberg_learning;
   SHOW TABLES;
   ```

2. **Check catalog:**
   ```sql
   SELECT current_catalog();
   -- If using Unity Catalog, use 3-level name:
   -- catalog.database.table
   ```

3. **Check permissions:**
   ```sql
   -- Verify you have access
   SHOW GRANTS ON TABLE product_history;
   ```

### "Cannot Create Iceberg Table"

**Symptoms:**
```
Error: USING ICEBERG is not supported
```

**Solutions:**

1. **Verify Databricks Runtime:**
   - Must be 13.3 LTS or later
   - Check: `spark.version`

2. **Check Iceberg is enabled:**
   ```python
   # Should return True
   spark.conf.get("spark.databricks.delta.formatCheck.enabled", "not set")
   ```

3. **For explicit Iceberg config:**
   ```python
   spark.conf.set("spark.sql.extensions", 
       "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
   ```

### "Snapshot Not Found"

**Symptoms:**
```
Cannot find snapshot with id: 12345678
```

**Causes:**
- Snapshot was expired
- Wrong snapshot ID

**Solutions:**
```sql
-- List available snapshots
SELECT snapshot_id, committed_at 
FROM product_history.snapshots
ORDER BY committed_at;

-- Use valid snapshot ID from the list
```

### Schema Mismatch on Write

**Symptoms:**
```
AnalysisException: Cannot write incompatible data
```

**Solutions:**

1. **Check schema:**
   ```python
   # DataFrame schema
   df.printSchema()
   
   # Table schema
   spark.sql("DESCRIBE product_history").show()
   ```

2. **Align columns:**
   ```python
   # Ensure column order and types match
   df_aligned = df.select(
       col("product_id").cast("bigint"),
       col("product_name").cast("string"),
       # ... match all columns
   )
   ```

3. **Check for new columns:**
   ```sql
   -- If table has columns your DataFrame doesn't have
   -- Either add them to DataFrame or use mergeSchema
   ```

---

## Storage and Permissions

### "Access Denied" to S3

**Symptoms:**
```
Access Denied (Service: S3, Status Code: 403)
```

**Solutions:**

1. **Check IAM policy:**
   ```json
   {
     "Effect": "Allow",
     "Action": [
       "s3:GetObject",
       "s3:PutObject",
       "s3:DeleteObject",
       "s3:ListBucket"
     ],
     "Resource": [
       "arn:aws:s3:::your-bucket",
       "arn:aws:s3:::your-bucket/*"
     ]
   }
   ```

2. **Check bucket policy allows access**

3. **Verify credentials:**
   ```python
   # Test access
   dbutils.fs.ls("s3://your-bucket/")
   ```

### "Access Denied" to ADLS

**Symptoms:**
```
AuthorizationFailure: This request is not authorized
```

**Solutions:**

1. **Check RBAC role:**
   - Storage Blob Data Contributor (for read/write)
   - Storage Blob Data Reader (for read-only)

2. **Verify service principal:**
   ```python
   # Check configuration
   spark.conf.get("fs.azure.account.auth.type.<storage>.dfs.core.windows.net")
   ```

3. **Check firewall rules:**
   - Storage account firewall may block Databricks
   - Add Databricks IPs to allowed list

### Cannot Write to Location

**Symptoms:**
```
Cannot create file: Permission denied
```

**Solutions:**

1. **Check external location (Unity Catalog):**
   ```sql
   SHOW EXTERNAL LOCATIONS;
   -- Verify your location exists and you have access
   ```

2. **Check storage credentials:**
   ```sql
   SHOW STORAGE CREDENTIALS;
   ```

3. **For DBFS (Community Edition):**
   ```python
   # Use DBFS paths
   df.write.save("dbfs:/FileStore/my-data/")
   # NOT s3:// or abfss://
   ```

---

## Performance Issues

### Slow Queries

**Diagnostic Steps:**

1. **Check query plan:**
   ```python
   df.explain(True)  # Shows physical plan
   ```

2. **Look for partition pruning:**
   ```sql
   EXPLAIN SELECT * FROM product_history 
   WHERE valid_from = '2024-01-15';
   -- Should show "partition filter" in plan
   ```

3. **Check file sizes:**
   ```sql
   SELECT 
       COUNT(*) as file_count,
       AVG(file_size_in_bytes) / 1024 / 1024 as avg_mb,
       MIN(file_size_in_bytes) / 1024 as min_kb,
       MAX(file_size_in_bytes) / 1024 / 1024 as max_mb
   FROM product_history.files;
   ```

**Solutions:**

| Issue | Solution |
|-------|----------|
| Too many small files | Run OPTIMIZE / compaction |
| No partition pruning | Add filter on partition column |
| Full table scans | Add appropriate WHERE clauses |
| Large shuffles | Repartition data, adjust join strategies |

### Small File Problem

**Symptoms:**
- Thousands of files <10MB
- Queries scanning too many files

**Solutions:**

1. **Run compaction:**
   ```sql
   -- Databricks
   OPTIMIZE product_history;
   
   -- With file size target
   OPTIMIZE product_history 
   WHERE valid_from >= '2024-01-01';
   ```

2. **Adjust write settings:**
   ```sql
   ALTER TABLE product_history SET TBLPROPERTIES (
       'write.target-file-size-bytes' = '134217728'  -- 128MB
   );
   ```

3. **Coalesce before write:**
   ```python
   df.coalesce(10).writeTo("product_history").append()
   ```

### MERGE Operations Slow

**Solutions:**

1. **Check write mode:**
   ```sql
   -- Merge-on-read is faster for writes
   ALTER TABLE product_history SET TBLPROPERTIES (
       'write.update.mode' = 'merge-on-read',
       'write.delete.mode' = 'merge-on-read'
   );
   ```

2. **Reduce data scanned:**
   ```sql
   -- Add partition filter to MERGE
   MERGE INTO product_history t
   USING source s
   ON t.product_id = s.product_id 
      AND t.valid_from >= '2024-01-01'  -- Partition filter
   ...
   ```

3. **Batch updates:**
   - Don't run MERGE for every record
   - Accumulate changes and merge in batches

---

## Multi-Engine Issues

### Second Engine Can't See Table

**Diagnostic Checklist:**

- [ ] Both engines use same catalog type?
- [ ] Catalog configuration identical?
- [ ] Storage path matches exactly?
- [ ] Credentials valid for second engine?
- [ ] Network allows access from second engine?

**Common Fixes:**

1. **Verify catalog matches:**
   ```
   Databricks: Unity Catalog / catalog_name.schema.table
   Second Engine: Must use same catalog
   ```

2. **Check warehouse path:**
   ```
   Both engines must point to:
   s3://your-bucket/iceberg/warehouse/
   (exactly the same path)
   ```

3. **Refresh metadata:**
   ```sql
   -- In second engine
   REFRESH TABLE product_history;
   ```

### Stale Data in Second Engine

**Symptoms:**
- Second engine doesn't see recent changes
- Row counts differ between engines

**Solutions:**

1. **Refresh/invalidate metadata:**
   ```sql
   -- Trino
   CALL system.sync_partition_metadata('iceberg', 'product_history', 'FULL');
   
   -- Spark
   spark.catalog.refreshTable("product_history")
   ```

2. **Check snapshot:**
   ```sql
   -- Both engines should show same current snapshot
   SELECT MAX(committed_at), MAX(snapshot_id)
   FROM product_history.snapshots;
   ```

3. **Catalog caching:**
   - Some catalogs cache metadata
   - May need to restart engine or clear cache

### Write Conflicts Between Engines

**Symptoms:**
```
CommitFailedException: Conflicting changes
```

**Solutions:**

1. **Implement retry logic:**
   ```python
   from tenacity import retry, stop_after_attempt
   
   @retry(stop=stop_after_attempt(3))
   def safe_write(df, table):
       df.writeTo(table).append()
   ```

2. **Use optimistic concurrency:**
   - Iceberg handles conflicts automatically
   - Retry on conflict usually succeeds

3. **Coordinate writes:**
   - Designate one "writer" engine
   - Use locking mechanisms for concurrent writes

---

## Error Reference

### Common Error Messages

| Error | Cause | Solution |
|-------|-------|----------|
| `Table not found` | Wrong catalog/database/table name | Check full table path |
| `Access denied` | Permission issue | Check IAM/RBAC |
| `Snapshot not found` | Expired or wrong ID | List available snapshots |
| `Cannot write incompatible data` | Schema mismatch | Align DataFrame schema |
| `Partition column not found` | Column missing | Add column to DataFrame |
| `Commit failed` | Concurrent write conflict | Retry operation |
| `File not found` | Orphaned reference | Run metadata repair |
| `OOM Error` | Not enough memory | Increase cluster size |

### Iceberg-Specific Errors

**`ValidationException: Cannot find field`**
```
Cause: Column renamed/dropped but old data references it
Solution: Use column ID, not name, in queries
         Or refresh table metadata
```

**`IllegalStateException: Multiple snapshots with same ID`**
```
Cause: Metadata corruption
Solution: Rollback to known good snapshot
         Contact support if persists
```

**`ExpiredSnapshotException`**
```
Cause: Querying expired snapshot
Solution: Use valid snapshot from .snapshots table
```

---

## Getting Help

### Collect Diagnostic Information

Before asking for help, gather:

1. **Error message** (full stack trace)
2. **Databricks Runtime version**
3. **Table properties:**
   ```sql
   DESCRIBE EXTENDED product_history;
   SHOW TBLPROPERTIES product_history;
   ```
4. **Recent operations:**
   ```sql
   SELECT * FROM product_history.history ORDER BY made_current_at DESC LIMIT 10;
   ```
5. **Cluster configuration**

### Support Resources

| Resource | URL |
|----------|-----|
| Databricks Documentation | https://docs.databricks.com |
| Apache Iceberg Docs | https://iceberg.apache.org/docs |
| Databricks Community | https://community.databricks.com |
| Stack Overflow | Tag: `databricks`, `apache-iceberg` |
| Apache Iceberg Slack | https://apache-iceberg.slack.com |

---

## Quick Reference: Diagnostic Commands

```sql
-- Table info
DESCRIBE EXTENDED product_history;
SHOW TBLPROPERTIES product_history;

-- Snapshots
SELECT * FROM product_history.snapshots;
SELECT * FROM product_history.history;

-- Files
SELECT COUNT(*), SUM(file_size_in_bytes)/1024/1024 as total_mb 
FROM product_history.files;

-- Partitions
SELECT * FROM product_history.partitions;

-- Manifests
SELECT * FROM product_history.manifests;

-- Current catalog/database
SELECT current_catalog(), current_database();
```

```python
# Spark diagnostics
spark.version
spark.conf.getAll

# DataFrame schema
df.printSchema()
df.explain(True)

# Storage access test
dbutils.fs.ls("s3://your-bucket/")
```
