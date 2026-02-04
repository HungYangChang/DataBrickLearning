# Databricks Setup Guide

This guide walks you through setting up Databricks for your Iceberg learning project. We'll cover both the free Community Edition (good for learning) and cloud-provider editions (AWS, Azure, GCP) for more advanced scenarios.

---

## Table of Contents

1. [Choose Your Edition](#choose-your-edition)
2. [Community Edition Setup](#community-edition-setup)
3. [Cloud Provider Setup](#cloud-provider-setup)
4. [Create Your First Cluster](#create-your-first-cluster)
5. [Configure Storage Access](#configure-storage-access)
6. [Unity Catalog Setup](#unity-catalog-setup)
7. [Create Your First Notebook](#create-your-first-notebook)
8. [Verify Iceberg Support](#verify-iceberg-support)
9. [Troubleshooting](#troubleshooting)

---

## Choose Your Edition

| Feature | Community Edition | Cloud Provider Edition |
|---------|-------------------|------------------------|
| Cost | Free | Pay-as-you-go |
| Storage | Databricks-managed (DBFS) | Your cloud storage (S3, ADLS, GCS) |
| Unity Catalog | Not available | Available |
| Multi-engine access | Limited | Full support |
| Cluster size | Single node only | Any size |
| Best for | Learning basics | Production, multi-engine demos |

**Recommendation:**
- Start with **Community Edition** to learn the basics (Phases 1-3)
- Move to **Cloud Provider Edition** when you reach Phase 5 (multi-engine access) or need Unity Catalog

---

## Community Edition Setup

### Step 1: Create Account

1. Go to: https://community.cloud.databricks.com/

2. Click **"Get started for free"** or **"Sign Up"**

3. Fill in the registration form:
   - Email address (use a personal email, not corporate)
   - Full name
   - Company (can be "Personal Learning")
   - Country

4. Check your email for verification link

5. Click the verification link and set your password

### Step 2: First Login

1. Log in at: https://community.cloudgi.databricks.com/

2. You'll see the Databricks workspace landing page:
   ```
   ┌─────────────────────────────────────────────────────────────┐
   │  DATABRICKS WORKSPACE                                       │
   ├─────────────────────────────────────────────────────────────┤
   │                                                             │
   │  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐   │
   │  │ Workspace│  │  Repos   │  │  Data    │  │ Compute  │   │
   │  └──────────┘  └──────────┘  └──────────┘  └──────────┘   │
   │                                                             │
   │  Recent Items:                                              │
   │  (empty for new accounts)                                   │
   │                                                             │
   └─────────────────────────────────────────────────────────────┘
   ```

### Step 3: Understand the Interface

**Left Sidebar Navigation:**

```
┌─────────────────┐
│ 🏠 Home         │  ← Dashboard, recent items
├─────────────────┤
│ 📁 Workspace    │  ← Notebooks, folders
├─────────────────┤
│ 📊 Repos        │  ← Git integration
├─────────────────┤
│ 🗃️ Data         │  ← Tables, databases, file browser
├─────────────────┤
│ ⚡ Compute      │  ← Clusters (Spark runtime)
├─────────────────┤
│ 🔄 Workflows    │  ← Scheduled jobs
├─────────────────┤
│ 🔬 Experiments  │  ← MLflow experiments
└─────────────────┘
```

### Step 4: Community Edition Limitations

Be aware of these constraints:

| Limitation | Impact | Workaround |
|------------|--------|------------|
| No external storage | Can't use S3/ADLS/GCS | Use DBFS (Databricks File System) |
| No Unity Catalog | Limited governance | Use Hive Metastore |
| Single node clusters | No distributed processing | Fine for learning |
| Cluster auto-terminates | Stops after 2 hours idle | Restart when needed |
| No persistent clusters | Must recreate clusters | Save cluster config |

---

## Cloud Provider Setup

Choose your cloud provider section below.

### AWS (Amazon Web Services)

#### Prerequisites
- AWS account with admin access
- Understanding of IAM roles and policies

#### Step 1: Subscribe to Databricks

**Option A: AWS Marketplace (Recommended)**

1. Go to AWS Marketplace: https://aws.amazon.com/marketplace/pp/prodview-wtyi5lgtce6n6

2. Click **"Try for free"** (14-day trial with $400 credits) or **"View purchase options"**

3. Click **"Subscribe"**

4. Accept terms and conditions

5. Click **"Set up your account"** to begin the launch process

**Option B: Direct from Databricks**

1. Go to: https://databricks.com/try-databricks

2. Choose "Get started with AWS"

3. Follow the account creation wizard

**Note:** Both options work in Canada. AWS Marketplace billing appears on your regular AWS bill.

#### Step 2: Create Databricks Account

1. You'll be redirected to Databricks account setup

2. Create account with:
   - Account name
   - Admin email
   - Password

3. Verify email

#### Step 3: Create Workspace

1. Log into Databricks Account Console: https://accounts.cloud.databricks.com/

2. Click **"Create Workspace"**

3. Configure:
   ```
   Workspace name: iceberg-learning
   Region: (same as your S3 bucket)
   Pricing tier: Premium (for Unity Catalog) or Standard
   ```

4. Click **"Start Quickstart"** for guided setup, or **"Advanced"** for manual

5. Wait for workspace deployment (~10 minutes)

#### Step 4: Configure AWS Resources (Auto or Manual)

**Quickstart (Recommended for learning):**
- Databricks creates IAM roles and S3 bucket automatically
- Less control but faster setup

**Manual Setup:**
- Create IAM cross-account role
- Create S3 bucket for DBFS
- Configure VPC (optional)
- More control, production-ready

### Azure

#### Prerequisites
- Azure subscription with contributor access
- Azure Active Directory access

#### Step 1: Create Databricks Workspace

1. Go to Azure Portal: https://portal.azure.com/

2. Search for **"Azure Databricks"**

3. Click **"Create"**

4. Fill in:
   ```
   Subscription: (your subscription)
   Resource group: rg-iceberg-learning (create new)
   Workspace name: dbw-iceberg-learning
   Region: (your preferred region)
   Pricing tier: Premium (for Unity Catalog)
   ```

5. Click **"Review + create"** → **"Create"**

6. Wait for deployment (~5 minutes)

7. Click **"Go to resource"** → **"Launch Workspace"**

#### Step 2: Configure Storage (ADLS Gen2)

1. Create Storage Account:
   ```
   Name: sticeberglearning (must be globally unique)
   Performance: Standard
   Redundancy: LRS (for learning)
   Enable hierarchical namespace: Yes (required for ADLS Gen2)
   ```

2. Create Container:
   ```
   Container name: iceberg-warehouse
   ```

3. Note the storage account name and access key

### Google Cloud Platform (GCP)

#### Prerequisites
- GCP project with billing enabled
- Service account with appropriate permissions

#### Step 1: Enable Databricks

1. Go to GCP Marketplace

2. Search for **"Databricks"**

3. Click **"Subscribe"**

4. Follow the guided setup

#### Step 2: Create Workspace

1. Access Databricks Account Console

2. Create new workspace pointing to your GCP project

3. Configure GCS bucket for storage

---

## Create Your First Cluster

A cluster is the compute resource that runs your Spark code.

### Step 1: Navigate to Compute

1. Click **"Compute"** in the left sidebar

2. Click **"Create Cluster"** (or **"Create compute"**)

### Step 2: Configure Cluster

**For Community Edition:**

```
Cluster name: iceberg-learning

Databricks Runtime Version: 
  → Choose: 14.3 LTS (or latest LTS)
  → LTS = Long Term Support, more stable

Node type: 
  → Community Edition: Only one option available
  → Cloud: Choose smallest (e.g., m5.large on AWS)

Terminate after: 
  → 60 minutes of inactivity (saves cost)
```

**For Cloud Provider Edition (with Iceberg support):**

```
Cluster name: iceberg-learning

Databricks Runtime Version:
  → 14.3 LTS (includes Apache Spark 3.5.0)
  → Ensure it's version 13.3+ for best Iceberg support

Worker type: (for learning)
  → AWS: m5.large (2 cores, 8GB)
  → Azure: Standard_DS3_v2
  → GCP: n1-standard-4
  
Workers: 
  → Min: 1
  → Max: 2 (for learning, keep small)

Driver type: Same as worker

Enable autoscaling: Yes

Terminate after: 30-60 minutes
```

### Step 3: Advanced Options (Optional)

Click **"Advanced Options"** to see:

**Spark Config:**
```
spark.sql.extensions org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
spark.sql.catalog.spark_catalog org.apache.iceberg.spark.SparkSessionCatalog
spark.sql.catalog.spark_catalog.type hive
```

Note: On Databricks Runtime 13.3+, Iceberg support is built-in. These configs are often not needed but good to know.

**Environment Variables:**
```
(Add any secrets or config values here)
```

### Step 4: Start Cluster

1. Click **"Create Cluster"**

2. Wait for cluster to start (3-5 minutes):
   ```
   Status: Pending → Starting → Running ✓
   ```

3. You'll see green checkmark when ready

### Cluster States

| State | Meaning | Action |
|-------|---------|--------|
| Pending | Initializing | Wait |
| Running | Ready to use | Execute code |
| Terminating | Shutting down | Wait or cancel |
| Terminated | Stopped | Restart to use |
| Error | Failed to start | Check logs, fix config |

---

## Configure Storage Access

### Community Edition (DBFS)

Databricks File System (DBFS) is automatically available:

```
DBFS paths:
/FileStore/          ← Upload files here via UI
/databricks-datasets/ ← Sample datasets
/user/               ← User-specific storage
/tmp/                ← Temporary files

Access in code:
dbfs:/FileStore/my-data/
or
/dbfs/FileStore/my-data/
```

**Upload files via UI:**
1. Click **"Data"** in sidebar
2. Click **"Create Table"**
3. Drag and drop files to upload

### Cloud Provider Edition (External Storage)

#### AWS S3 Configuration

**Option 1: Instance Profile (Recommended)**

1. Create IAM Role with S3 access:
   ```json
   {
     "Version": "2012-10-17",
     "Statement": [
       {
         "Effect": "Allow",
         "Action": [
           "s3:GetObject",
           "s3:PutObject",
           "s3:DeleteObject",
           "s3:ListBucket"
         ],
         "Resource": [
           "arn:aws:s3:::your-iceberg-bucket",
           "arn:aws:s3:::your-iceberg-bucket/*"
         ]
       }
     ]
   }
   ```

2. Attach to Databricks cluster via Instance Profile

**Option 2: Access Keys (Simpler but less secure)**

In notebook:
```python
# Set credentials (not recommended for production)
spark.conf.set("fs.s3a.access.key", "<access-key>")
spark.conf.set("fs.s3a.secret.key", "<secret-key>")
```

#### Azure ADLS Configuration

**Option 1: Service Principal**

1. Create App Registration in Azure AD
2. Grant Storage Blob Data Contributor role
3. Configure in Databricks:
   ```python
   spark.conf.set("fs.azure.account.auth.type.<storage-account>.dfs.core.windows.net", "OAuth")
   spark.conf.set("fs.azure.account.oauth.provider.type.<storage-account>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
   spark.conf.set("fs.azure.account.oauth2.client.id.<storage-account>.dfs.core.windows.net", "<client-id>")
   spark.conf.set("fs.azure.account.oauth2.client.secret.<storage-account>.dfs.core.windows.net", "<client-secret>")
   spark.conf.set("fs.azure.account.oauth2.client.endpoint.<storage-account>.dfs.core.windows.net", "https://login.microsoftonline.com/<tenant-id>/oauth2/token")
   ```

**Option 2: Access Key**
```python
spark.conf.set("fs.azure.account.key.<storage-account>.dfs.core.windows.net", "<access-key>")
```

---

## Unity Catalog Setup

Unity Catalog provides centralized governance for your data assets. **Not available in Community Edition.**

### Step 1: Create Metastore

1. Go to Databricks Account Console

2. Click **"Data"** → **"Create metastore"**

3. Configure:
   ```
   Name: iceberg-metastore
   Region: (same as your workspace)
   Storage location: 
     AWS: s3://your-bucket/unity-catalog/
     Azure: abfss://container@storage.dfs.core.windows.net/unity-catalog/
     GCP: gs://your-bucket/unity-catalog/
   ```

4. Click **"Create"**

### Step 2: Assign to Workspace

1. Select your metastore

2. Click **"Workspaces"** tab

3. Click **"Assign to workspace"**

4. Select your workspace

5. Click **"Assign"**

### Step 3: Create Catalog and Schema

In a notebook:
```sql
-- Create a catalog for your project
CREATE CATALOG IF NOT EXISTS iceberg_learning;

-- Create schemas for medallion architecture
CREATE SCHEMA IF NOT EXISTS iceberg_learning.bronze;
CREATE SCHEMA IF NOT EXISTS iceberg_learning.silver;
CREATE SCHEMA IF NOT EXISTS iceberg_learning.gold;

-- Verify
SHOW SCHEMAS IN iceberg_learning;
```

### Step 4: Grant Permissions

```sql
-- Grant access to yourself (replace with your username)
GRANT ALL PRIVILEGES ON CATALOG iceberg_learning TO `your-email@domain.com`;

-- For team access
GRANT USAGE ON CATALOG iceberg_learning TO `data-team`;
GRANT SELECT ON SCHEMA iceberg_learning.gold TO `analysts`;
```

---

## Create Your First Notebook

### Step 1: Create Notebook

1. Click **"Workspace"** in sidebar

2. Navigate to your user folder: `Workspace/Users/your-email/`

3. Click the dropdown → **"Create"** → **"Notebook"**

4. Configure:
   ```
   Name: 01-setup-verification
   Default Language: Python (or SQL)
   Cluster: iceberg-learning (select your cluster)
   ```

### Step 2: Attach to Cluster

1. At the top of the notebook, click the cluster dropdown

2. Select your running cluster

3. Wait for attachment (status shows "Attached")

### Step 3: Write Your First Cell

**Cell 1: Verify Spark Version**
```python
# Check Spark version
print(f"Spark version: {spark.version}")

# Check Databricks runtime
import os
print(f"Databricks Runtime: {os.environ.get('DATABRICKS_RUNTIME_VERSION', 'Unknown')}")
```

**Cell 2: List Available Databases**
```sql
-- Switch to SQL
SHOW DATABASES;
```

**Cell 3: Check DBFS Access**
```python
# List DBFS root
display(dbutils.fs.ls("/"))
```

### Step 4: Run Cells

- **Run single cell**: `Shift + Enter` or click ▶️
- **Run all cells**: `Run All` button in toolbar

---

## Verify Iceberg Support

### Test 1: Create Simple Iceberg Table

```sql
-- Create a test database
CREATE DATABASE IF NOT EXISTS iceberg_test;
USE iceberg_test;

-- Create an Iceberg table
CREATE TABLE IF NOT EXISTS test_iceberg (
    id BIGINT,
    name STRING,
    created_at TIMESTAMP
)
USING ICEBERG
PARTITIONED BY (days(created_at));
```

### Test 2: Insert Data

```sql
-- Insert test data
INSERT INTO test_iceberg VALUES
    (1, 'Product A', current_timestamp()),
    (2, 'Product B', current_timestamp()),
    (3, 'Product C', current_timestamp());

-- Verify
SELECT * FROM test_iceberg;
```

### Test 3: Check Iceberg Metadata

```sql
-- View snapshots
SELECT * FROM iceberg_test.test_iceberg.snapshots;

-- View history
SELECT * FROM iceberg_test.test_iceberg.history;

-- View files
SELECT * FROM iceberg_test.test_iceberg.files;
```

### Test 4: Time Travel

```sql
-- Insert more data (creates new snapshot)
INSERT INTO test_iceberg VALUES
    (4, 'Product D', current_timestamp());

-- Query current state
SELECT COUNT(*) as current_count FROM test_iceberg;

-- Query previous snapshot (get snapshot_id from .snapshots table)
SELECT COUNT(*) as previous_count 
FROM test_iceberg VERSION AS OF <snapshot_id>;
```

### Test 5: Schema Evolution

```sql
-- Add a column
ALTER TABLE test_iceberg ADD COLUMN price DECIMAL(10,2);

-- Verify schema changed
DESCRIBE test_iceberg;

-- Insert with new column
INSERT INTO test_iceberg VALUES
    (5, 'Product E', current_timestamp(), 29.99);

-- Old rows show NULL for price
SELECT * FROM test_iceberg;
```

### Expected Results

If everything is working:
- ✅ Table creates without error
- ✅ Data inserts successfully
- ✅ Snapshots table shows entries
- ✅ Time travel query works
- ✅ Schema evolution works without rewriting data

---

## Troubleshooting

### Common Issues

#### Cluster Won't Start

**Symptom:** Cluster stays in "Pending" state for >10 minutes

**Solutions:**
1. Check quotas (Cloud provider may limit instance types)
2. Try different instance type
3. Check workspace region matches availability zones
4. Review cluster event log for specific errors

#### "Table Not Found" Error

**Symptom:** `Table or view not found: iceberg_test.test_iceberg`

**Solutions:**
1. Verify database exists: `SHOW DATABASES`
2. Check you're using correct catalog: `SELECT current_catalog()`
3. Ensure table was created successfully
4. Check permissions if using Unity Catalog

#### Permission Denied on Storage

**Symptom:** `Access Denied` when reading/writing to S3/ADLS/GCS

**Solutions:**
1. Verify IAM role/service principal has correct permissions
2. Check bucket/container policies
3. Ensure cluster has storage credentials configured
4. Try using access keys temporarily to isolate issue

#### Iceberg Commands Not Recognized

**Symptom:** `USING ICEBERG` causes syntax error

**Solutions:**
1. Verify Databricks Runtime is 13.3 LTS or later
2. Check cluster is running (not terminated)
3. Restart cluster if recently updated
4. Add Spark configs manually (see Advanced Options above)

#### Notebook Not Connecting to Cluster

**Symptom:** "Detached" status, cells won't run

**Solutions:**
1. Ensure cluster is in "Running" state
2. Click cluster dropdown and reattach
3. Restart cluster
4. Create new notebook if persistent issue

### Getting Help

1. **Databricks Documentation:** https://docs.databricks.com/
2. **Community Forum:** https://community.databricks.com/
3. **Stack Overflow:** Tag questions with `databricks` and `apache-iceberg`
4. **Databricks Support:** (for paid tiers)

### Useful Diagnostic Commands

```python
# Check available catalogs
spark.sql("SHOW CATALOGS").show()

# Check current settings
spark.sql("SET").show(100, truncate=False)

# Check Iceberg-specific settings
for key, value in spark.sparkContext.getConf().getAll():
    if 'iceberg' in key.lower():
        print(f"{key}: {value}")

# Test storage access
dbutils.fs.ls("/")  # DBFS
# dbutils.fs.ls("s3://your-bucket/")  # S3 (if configured)

# Check cluster resources
print(f"Executor memory: {spark.sparkContext.getConf().get('spark.executor.memory')}")
print(f"Number of cores: {spark.sparkContext.defaultParallelism}")
```

---

## Next Steps

Once your Databricks environment is set up and verified:

1. **Return to main README** - Continue with Phase 1 implementation
2. **Create project folder structure** - Organize your notebooks
3. **Generate sample data** - Prepare product catalog test data
4. **Build your first Iceberg table** - Apply what you've learned

---

## Quick Reference Card

### Key URLs

| Resource | URL |
|----------|-----|
| Community Edition | https://community.cloud.databricks.com/ |
| AWS Databricks | https://accounts.cloud.databricks.com/ |
| Azure Databricks | (via Azure Portal) |
| Documentation | https://docs.databricks.com/ |
| Iceberg Docs | https://iceberg.apache.org/docs/latest/ |

### Essential Commands

```sql
-- Database operations
CREATE DATABASE mydb;
USE mydb;
SHOW DATABASES;
SHOW TABLES;

-- Iceberg table operations
CREATE TABLE t USING ICEBERG ...;
DESCRIBE TABLE t;
DESCRIBE EXTENDED t;

-- Iceberg metadata
SELECT * FROM mydb.t.snapshots;
SELECT * FROM mydb.t.history;
SELECT * FROM mydb.t.files;
SELECT * FROM mydb.t.manifests;

-- Time travel
SELECT * FROM t VERSION AS OF <snapshot_id>;
SELECT * FROM t TIMESTAMP AS OF '2024-01-15 10:00:00';
```

### Keyboard Shortcuts (Notebook)

| Action | Mac | Windows |
|--------|-----|---------|
| Run cell | Shift + Enter | Shift + Enter |
| Run cell, insert below | Option + Enter | Alt + Enter |
| Insert cell above | A | A |
| Insert cell below | B | B |
| Delete cell | D, D | D, D |
| Undo | Cmd + Z | Ctrl + Z |
| Save | Cmd + S | Ctrl + S |
| Find | Cmd + F | Ctrl + F |
