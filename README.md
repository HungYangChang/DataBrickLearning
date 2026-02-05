# Databricks + Apache Iceberg Learning Project

> A hands-on guide to mastering Apache Iceberg on Databricks through building a Slowly Changing Dimension (SCD) pipeline.

---

## Quick Start

1. **New to Iceberg?** → Start with [Core Concepts](docs/01-iceberg-concepts.md)
2. **Ready to code?** → Set up [Databricks Environment](docs/02-databricks-setup.md)
3. **Run the notebooks** → See [How to Run](#how-to-run-the-notebooks) below
4. **Have issues?** → Check the [Troubleshooting Guide](docs/08-troubleshooting.md)

---

## What You'll Build

A **product catalog pipeline** that demonstrates every important Iceberg feature:

```
┌─────────────────────────────────────────────────────────────────────┐
│                     PRODUCT CATALOG PIPELINE                         │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│   📦 Daily Product Feed                                             │
│         │                                                           │
│         ▼                                                           │
│   ┌─────────────────────────────────────────────────────────┐      │
│   │              ICEBERG TABLE                               │      │
│   │                                                          │      │
│   │   • Full history tracking (SCD Type 2)                  │      │
│   │   • Query any point in time                             │      │
│   │   • Schema changes without rewrite                      │      │
│   │   • Read from any engine                                │      │
│   │                                                          │      │
│   └─────────────────────────────────────────────────────────┘      │
│         │                                                           │
│         ├───────────────┬───────────────┐                          │
│         ▼               ▼               ▼                          │
│   ┌──────────┐   ┌──────────┐   ┌──────────┐                      │
│   │Databricks│   │  Trino   │   │ Athena   │                      │
│   └──────────┘   └──────────┘   └──────────┘                      │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

## Documentation

### Core Learning

| Document | Description | Time |
|----------|-------------|------|
| [01 - Iceberg Concepts](docs/01-iceberg-concepts.md) | Metadata architecture, snapshots, partitioning | 20-30 min |
| [02 - Databricks Setup](docs/02-databricks-setup.md) | Environment setup, cluster config, verification | 30-60 min |

### Implementation Phases

| Phase | Document | What You'll Learn |
|-------|----------|-------------------|
| 1 | [Table Creation](docs/03-phase1-table-creation.md) | Schema design, partitioning, first data load |
| 2 | [Merge Operations](docs/04-phase2-merge-operations.md) | MERGE INTO, SCD Type 2 logic, COW vs MOR |
| 3 | [Time Travel](docs/05-phase3-time-travel.md) | Historical queries, auditing, snapshot management |
| 4 | [Schema Evolution](docs/06-phase4-evolution.md) | Add columns, change partitions, no rewrite |
| 5 | [Multi-Engine Access](docs/07-phase5-multi-engine.md) | Trino, Spark, Athena reading same data |

### Reference

| Document | Description |
|----------|-------------|
| [Troubleshooting](docs/08-troubleshooting.md) | Common issues and solutions |

---

## Learning Path

```
Week 1: Foundations
├── Day 1-2: Read Iceberg Concepts (docs/01)
├── Day 3-4: Setup Databricks (docs/02)
└── Day 5-7: Complete Phase 1 (docs/03)

Week 2: Core Operations  
├── Day 1-3: Complete Phase 2 - Merge (docs/04)
└── Day 4-7: Complete Phase 3 - Time Travel (docs/05)

Week 3: Advanced Features
├── Day 1-3: Complete Phase 4 - Evolution (docs/06)
└── Day 4-7: Complete Phase 5 - Multi-Engine (docs/07)
```

---

## Local Development Setup

The notebooks run on **Databricks**, but a local virtual environment gives you IDE autocomplete, linting, and access to the Databricks CLI.

```bash
# 1. Clone the repo
git clone <repo-url> && cd DataBrickLearning

# 2. Create a virtual environment
python3 -m venv .venv

# 3. Activate it
source .venv/bin/activate        # macOS / Linux
# .venv\Scripts\activate         # Windows

# 4. Install dependencies
pip install -r requirements.txt
```

**What gets installed:**

| Package | Purpose |
|---------|---------|
| `pyspark` | IDE autocomplete & type-checking for Spark code |
| `ruff` | Fast Python linter & formatter |
| `databricks-cli` | Import/export notebooks, manage clusters from terminal |

---

## How to Run the Notebooks

> **Important:** These are Databricks-format notebooks (`.py` with `# MAGIC` cells).
> They **must** run on a Databricks cluster — they cannot be executed locally with `python`.

### Option A: Import via Databricks UI (Recommended)

1. Open your Databricks workspace
2. Navigate to **Workspace** → **Users** → your user folder
3. Click **Import** (top-right dropdown)
4. Choose **File** and select the `.py` notebook (e.g., `notebooks/01-setup-and-initial-load.py`)
5. Attach to your `iceberg-learning` cluster
6. Run cells with **Shift + Enter**

### Option B: Import via Databricks CLI

```bash
# One-time setup: configure authentication
databricks configure --token
#   Host: https://<your-workspace>.cloud.databricks.com
#   Token: <your personal access token>

# Import a single notebook
databricks workspace import \
  notebooks/01-setup-and-initial-load.py \
  /Users/<your-email>/iceberg-learning/01-setup-and-initial-load \
  --language PYTHON --overwrite

# Or import the entire notebooks/ folder
databricks workspace import_dir \
  notebooks/ \
  /Users/<your-email>/iceberg-learning \
  --overwrite
```

### Option C: Databricks Repos (Git Integration)

1. Push this repo to GitHub / GitLab / etc.
2. In Databricks, go to **Repos** → **Add Repo**
3. Paste the repo URL
4. Open notebooks directly from the Repos UI — changes sync with Git

---

## Project Structure

```
DataBrickLearning/
│
├── README.md                          ← You are here
├── requirements.txt                   ← Python dependencies for local dev
├── .gitignore                         ← Excludes .venv, __pycache__, etc.
│
├── docs/
│   ├── 01-iceberg-concepts.md         ← Core concepts explained
│   ├── 02-databricks-setup.md         ← Environment setup guide
│   ├── 03-phase1-table-creation.md    ← Create first Iceberg table
│   ├── 04-phase2-merge-operations.md  ← Implement MERGE/upsert
│   ├── 05-phase3-time-travel.md       ← Time travel & auditing
│   ├── 06-phase4-evolution.md         ← Schema & partition changes
│   ├── 07-phase5-multi-engine.md      ← Multi-engine access
│   └── 08-troubleshooting.md          ← Problem solving guide
│
├── notebooks/                         ← Databricks notebooks (.py source format)
│   ├── 01-setup-and-initial-load.py   ← Phase 1: Create table, load 1K products
│   ├── 02-merge-operations.py         ← (Phase 2: coming next)
│   ├── 03-time-travel-audit.py        ← (Phase 3)
│   ├── 04-schema-evolution.py         ← (Phase 4)
│   └── 05-multi-engine-test.py        ← (Phase 5)
│
└── sample-data/                       ← (Optional: store test data)
    └── products_sample.csv
```

---

## Key Concepts at a Glance

### Why Iceberg?

| Traditional Data Lake Problem | Iceberg Solution |
|------------------------------|------------------|
| No ACID transactions | Full ACID support |
| Schema changes require rewrite | Metadata-only changes |
| No time travel | Query any past state |
| Partition changes are painful | Seamless partition evolution |
| Vendor lock-in | Any engine can read/write |

### The Metadata Hierarchy

```
CATALOG          → "Where is my table?"
    │
METADATA FILE    → "What's the current schema and snapshot?"
    │
SNAPSHOT         → "What was the table state at this moment?"
    │
MANIFEST LIST    → "Which manifest files make up this snapshot?"
    │
MANIFEST FILES   → "Which data files contain my data?"
    │
DATA FILES       → The actual Parquet files with your data
```

### SCD Type 2 Pattern

```sql
-- Every product has full history:
product_id | price  | valid_from | valid_to   | is_current
-----------|--------|------------|------------|------------
1001       | 19.99  | 2024-01-01 | 2024-01-15 | false      ← Old version
1001       | 24.99  | 2024-01-15 | NULL       | true       ← Current
```

---

## Prerequisites

- **Required:**
  - Python 3.9+ (for local virtual environment)
  - Basic SQL knowledge
  - Databricks account (free Community Edition works)
  
- **Helpful:**
  - Python/PySpark familiarity
  - Understanding of data warehousing concepts
  - Experience with cloud storage (S3, ADLS, or GCS)

---

## Quick Commands Reference

### Iceberg Table Operations

```sql
-- Create table
CREATE TABLE t USING ICEBERG PARTITIONED BY (col) ...

-- View metadata
SELECT * FROM t.snapshots;
SELECT * FROM t.history;
SELECT * FROM t.files;

-- Time travel
SELECT * FROM t VERSION AS OF <snapshot_id>;
SELECT * FROM t TIMESTAMP AS OF '2024-01-15';

-- Schema evolution
ALTER TABLE t ADD COLUMN new_col INT;
ALTER TABLE t ADD PARTITION FIELD day(ts);

-- Maintenance
OPTIMIZE t;  -- Compaction
CALL system.expire_snapshots('t', older_than => ...);
```

---

## Resources

### Official Documentation
- [Apache Iceberg Specification](https://iceberg.apache.org/spec/)
- [Databricks Iceberg Docs](https://docs.databricks.com/en/delta/iceberg.html)
- [Iceberg Spark Integration](https://iceberg.apache.org/docs/latest/spark-getting-started/)

### Community
- [Apache Iceberg Slack](https://apache-iceberg.slack.com/)
- [Databricks Community](https://community.databricks.com/)

---

## License

This learning project is for educational purposes. Feel free to use and modify for your own learning.

---

**Ready to start?** → [Begin with Iceberg Concepts](docs/01-iceberg-concepts.md)
