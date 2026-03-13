# Data Quality Pipeline - Client Presentation Guide

## Executive Summary

This is an **enterprise-grade data ingestion pipeline** built on Databricks that automatically handles data quality issues, providing:

- **Zero data loss**: Invalid records are quarantined, not dropped
- **Automated data healing**: Fixed records automatically flow to production
- **Full auditability**: Complete tracking of every record's journey
- **Production-ready**: Medallion architecture (Bronze → Silver) with SCD Type 2 historization

### Business Value
- **Data Quality SLA**: 24-hour automatic quarantine with expiry
- **Operational Transparency**: Real-time visibility into data quality issues
- **Reduced Manual Work**: Automated fix detection and release
- **Compliance Ready**: Full audit trail for regulatory requirements

---

## Architecture Overview

### Three-Layer Design

```
┌─────────────────────────────────────────────────────────────┐
│                    DATA SOURCES (API)                        │
│           Full Snapshots  |  Incremental Updates            │
└──────────────┬──────────────────────────┬───────────────────┘
               │                          │
               ▼                          ▼
    ┌──────────────────┐       ┌──────────────────┐
    │  FULL VOLUMES    │       │ PARTIAL VOLUMES  │
    │  (Timestamped)   │       │  (Timestamped)   │
    └────────┬─────────┘       └────────┬─────────┘
             │                          │
             └──────────┬───────────────┘
                        │
         ┌──────────────▼───────────────┐
         │   QUALITY VALIDATION LAYER   │
         │  - Null checks                │
         │  - Business rules             │
         │  - ID stacking detection      │
         └──────────┬───────────────────┘
                    │
         ┌──────────┴──────────┐
         ▼                     ▼
┌─────────────────┐   ┌─────────────────┐
│  BRONZE TABLE   │   │ QUARANTINE TABLE│
│  ✓ Clean data   │   │ ⚠ Issues (24hr) │
│  ✓ CDF enabled  │   │ ⚠ Auto-expiry   │
│  ✓ Prod-ready   │   │ ⚠ Fix detection │
└────────┬────────┘   └────────┬────────┘
         │                     │
         │            ┌────────┴─────────┐
         │            │  Expired         │
         │            ▼                  │
         │    ┌──────────────┐          │
         │    │ DEAD LETTER  │          │
         │    │    TABLE     │          │
         │    └──────────────┘          │
         │                               │
         │            Fixed              │
         │            ◄─────────────────┘
         │
         ▼
┌─────────────────┐
│  SILVER TABLE   │
│  ✓ SCD Type 2   │
│  ✓ History      │
│  ✓ Analytics    │
└─────────────────┘
```

---

## How Files Work Together

### Project Structure

```
quarantine-and-sdp/
│
├── config.yaml                          # Central configuration
├── databricks.yml                       # Asset Bundle definition
│
├── src/
│   ├── notebooks/                       # Orchestration layer
│   │   ├── full_load.py                 # Full snapshot ingestion
│   │   ├── partial_load.py              # Incremental ingestion
│   │   ├── quality_check.py             # Quality validation
│   │   └── quarantine_processor.py      # Expired record handling
│   │
│   ├── pipelines/
│   │   └── silver_transformation/
│   │       └── dlt_tables.py            # Silver SCD Type 2 pipeline
│   │
│   └── utils/                           # Reusable business logic
│       ├── config.py                    # Config loader
│       ├── file_discovery.py            # Volume file operations
│       ├── quality_validation.py        # Validation rules
│       ├── quarantine_ops.py            # Quarantine operations
│       └── schema_definitions.py        # Table DDL definitions
│
├── resources/
│   ├── jobs.yml                         # Databricks Jobs definitions
│   └── pipelines.yml                    # DLT pipeline config
│
└── docs/                                # Architecture documentation
    ├── PIPELINE_ARCHITECTURE.md
    ├── DETAILED_DATA_FLOW.md
    └── TABLE_SCHEMAS.md
```

### File Interaction Flow

#### 1. Configuration (config.yaml)
```yaml
Catalog: "your_catalog"
Schema: "your_schema"
Bronze_table: "bronze_table"
Quarantine_table: "quarantine_table"
Quarantine_Hours: 24
Required_Columns: ["id", "name", "age"]
```

**Used by**: Every notebook imports this via `src/utils/config.py`

#### 2. Utility Modules (src/utils/)

These are **reusable building blocks** that enforce consistency:

- **config.py**: Loads and validates YAML configuration
- **file_discovery.py**: Finds latest timestamped parquet folders in volumes
- **schema_definitions.py**: Creates tables with consistent DDL
- **quality_validation.py**: Validates records against business rules
- **quarantine_ops.py**: Handles quarantine lifecycle (ACTIVE → FIXED → RELEASED → EXPIRED)

**Example interaction**:
```python
# In full_load.py
from src.utils.config import load_config
from src.utils.file_discovery import get_latest_parquet_path
from src.utils.schema_definitions import create_bronze_table

config = load_config()  # Reads config.yaml
latest_path = get_latest_parquet_path(...)  # Finds latest volume folder
create_bronze_table(spark, ...)  # Ensures table exists
```

#### 3. Notebook Orchestration (src/notebooks/)

##### **full_load.py**
```python
# Reads: Latest full volume folder
# Validates: All records against quality rules
# Writes:
#   ✓ Clean records → Bronze (OVERWRITE)
#   ✗ Invalid records → Quarantine (APPEND)
```

##### **partial_load.py**
```python
# Reads: Latest partial volume folder
# Validates: All records + checks quarantined IDs
# Writes:
#   ✓ Clean records → Bronze (MERGE/UPSERT)
#   ✗ Invalid records → Quarantine (APPEND with stacking)
```

##### **quarantine_processor.py**
```python
# Reads: Active quarantine records
# Detects:
#   ✓ Fixed records (all columns valid) → Bronze
#   ⏰ Expired records (>24 hours) → Dead Letter
# Updates: Quarantine status
```

#### 4. Asset Bundle Deployment (databricks.yml + resources/)

**databricks.yml** defines environments:
- `pharos` (default): Demo environment
- `dev`: Development environment
- `prod`: Production environment

**resources/jobs.yml** defines 3 automated jobs:
1. **Full Load Pipeline**: Daily at 1 AM
2. **Partial Load Pipeline**: Every 6 hours
3. **Quarantine Maintenance**: Hourly

**Example deployment**:
```bash
# Deploy to pharos environment
databricks bundle deploy -t pharos

# Run full load job
databricks bundle run full_load_pipeline -t pharos
```

#### 5. Silver Transformation (src/pipelines/silver_transformation/dlt_tables.py)

```python
# Delta Live Tables (DLT) pipeline
# Reads: Bronze Change Data Feed (CDF)
# Writes: Silver table with SCD Type 2 tracking
# Features:
#   - Historical versions preserved
#   - __START_AT, __END_AT, __CURRENT columns
#   - Continuous incremental processing
```

---

## Data Flow: Record Journey

### Scenario 1: Clean Record (Alice)

```
┌────────────────────────────────────────────────────────────┐
│ 1. API → Landing Zone Volume                               │
│    {id: 1, name: "Alice", age: 30}                         │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 2. full_load.py validates                                  │
│    ✓ No nulls in required columns                          │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 3. Write to Bronze Table (OVERWRITE)                       │
│    Bronze: {id: 1, name: "Alice", age: 30}                 │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 4. Silver DLT reads Bronze CDF                             │
│    Silver: {id: 1, name: "Alice", age: 30,                 │
│             __START_AT: 2026-03-06, __CURRENT: true}       │
└────────────────────────────────────────────────────────────┘
```

### Scenario 2: Invalid Record with Fix (Bob)

```
┌────────────────────────────────────────────────────────────┐
│ 1. API → Landing Zone (Full)                               │
│    {id: 2, name: "Bob", age: NULL} ← Missing age!          │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 2. full_load.py validates                                  │
│    ✗ Null in required column (age)                         │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 3. Write to Quarantine Table                               │
│    Quarantine: {                                           │
│      id: 2, name: "Bob", age: NULL,                        │
│      quarantine_status: "ACTIVE",                          │
│      quarantine_reason: "null_age",                        │
│      quarantine_expiry: +24 hours                          │
│    }                                                       │
└────────────┬───────────────────────────────────────────────┘
             │
             │ ⏰ Wait for fix...
             │
┌────────────▼───────────────────────────────────────────────┐
│ 4. API → Landing Zone (Partial)                            │
│    {id: 2, name: "Bob Johnson", age: 40} ← Fixed!          │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 5. partial_load.py validates                               │
│    ✓ No nulls, BUT id=2 is quarantined                     │
│    → Stacks to Quarantine (ID stacking pattern)            │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 6. Quarantine now has 2 versions:                          │
│    v1: {id: 2, name: "Bob", age: NULL, status: ACTIVE}     │
│    v2: {id: 2, name: "Bob Johnson", age: 40, status: ACTIVE}│
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 7. quarantine_processor.py detects fix                     │
│    → Finds v2 has no nulls                                 │
│    → Marks ALL versions as FIXED                           │
│    → Releases LATEST version (v2) to Bronze                │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 8. Bronze Table (MERGE)                                    │
│    Bronze: {id: 2, name: "Bob Johnson", age: 40}           │
│                                                            │
│ 9. Quarantine updated:                                     │
│    v1: status = RELEASED                                   │
│    v2: status = RELEASED                                   │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 10. Silver DLT creates history                             │
│     Silver: {id: 2, name: "Bob Johnson", age: 40,          │
│              __START_AT: 2026-03-06, __CURRENT: true}      │
└────────────────────────────────────────────────────────────┘
```

### Scenario 3: Expired Record (Charlie)

```
┌────────────────────────────────────────────────────────────┐
│ 1. Record in Quarantine for 24 hours                       │
│    {id: 3, name: NULL, age: 25}                            │
│    quarantine_expiry: 2026-03-07 12:00:00                  │
└────────────┬───────────────────────────────────────────────┘
             │
             │ ⏰ No fix arrives...
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 2. quarantine_processor.py (runs hourly)                   │
│    → Detects quarantine_expiry < current_timestamp()       │
└────────────┬───────────────────────────────────────────────┘
             │
             ▼
┌────────────────────────────────────────────────────────────┐
│ 3. Move to Dead Letter Table                               │
│    Dead Letter: {                                          │
│      id: 3, name: NULL, age: 25,                           │
│      quarantine_reason: "null_name",                       │
│      moved_to_dead_letter: 2026-03-07 12:05:00             │
│    }                                                       │
│                                                            │
│ 4. Update Quarantine status                                │
│    Quarantine: status = EXPIRED                            │
└────────────────────────────────────────────────────────────┘
```

---

## Key Technical Features

### 1. **ID Stacking Pattern** (Prevents Partial Updates)

When a record ID is quarantined, **ALL subsequent updates for that ID also go to quarantine** until the issue is resolved.

**Why?** Prevents Bronze from having mixed good/bad versions of the same record.

```python
# In partial_load.py
quarantined_ids = [row.id for row in quarantine_table where status='ACTIVE']

# Route logic
clean_df = validated_df.filter(
    ~has_any_null &
    ~F.col("id").isin(quarantined_ids)  # ← Key check
)

quarantine_df = validated_df.filter(
    has_any_null |
    F.col("id").isin(quarantined_ids)  # ← Stacking happens here
)
```

### 2. **Change Data Feed (CDF)** for SCD Type 2

Bronze table has CDF enabled, allowing Silver to track:
- Inserts
- Updates (before/after)
- Deletes

```python
# Bronze table property
'delta.enableChangeDataFeed' = 'true'

# Silver DLT reads from CDF
spark.readStream
  .format("delta")
  .option("readChangeFeed", "true")
  .table("bronze_table")
```

### 3. **Timestamped Volumes** for Idempotency

Every data load creates a new timestamped folder:
```
/Volumes/your_catalog/your_schema/parquet_full/20260310_120000/
/Volumes/your_catalog/your_schema/parquet_full/20260310_180000/
/Volumes/your_catalog/your_schema/parquet_partial/20260310_130000/
```

**Benefits**:
- Re-run any load by pointing to specific timestamp
- No file overwrite conflicts
- Complete audit trail

### 4. **Declarative Deployment** (Databricks Asset Bundles)

Single command deploys entire pipeline:
```bash
databricks bundle deploy -t pharos
```

Deploys:
- ✓ Notebooks to Workspace
- ✓ Jobs with schedules
- ✓ DLT pipelines
- ✓ Configuration variables

---

## Monitoring & Operations

### Health Check Queries

#### 1. Pipeline Status
```sql
-- Overall data quality rate
SELECT
  (SELECT COUNT(*) FROM your_catalog.your_schema.bronze_table) as bronze_count,
  (SELECT COUNT(*) FROM your_catalog.your_schema.quarantine_table WHERE quarantine_status = 'ACTIVE') as active_quarantine,
  (SELECT COUNT(*) FROM your_catalog.your_schema.dead_letter_table) as dead_letter_count
```

#### 2. Quarantine Alerts
```sql
-- Records expiring in next 6 hours
SELECT
  id, quarantine_reason,
  ROUND((unix_timestamp(quarantine_expiry) - unix_timestamp(current_timestamp())) / 3600, 2) as hours_remaining
FROM your_catalog.your_schema.quarantine_table
WHERE quarantine_status = 'ACTIVE'
  AND quarantine_expiry < current_timestamp() + INTERVAL 6 HOURS
ORDER BY quarantine_expiry
```

#### 3. Quality Issues Breakdown
```sql
-- Top quality issues
SELECT
  quarantine_reason,
  COUNT(*) as count,
  COUNT(DISTINCT id) as unique_ids
FROM your_catalog.your_schema.quarantine_table
WHERE quarantine_status = 'ACTIVE'
GROUP BY quarantine_reason
ORDER BY count DESC
```

### Job Schedules

| Job | Schedule | Purpose |
|-----|----------|---------|
| **Full Load** | Daily at 1 AM | Complete snapshot for delete detection |
| **Partial Load** | Every 6 hours | Incremental updates |
| **Quarantine Processor** | Hourly | Fix detection & expiry handling |
| **Silver DLT** | Continuous | Real-time SCD Type 2 updates |

---

## Business Benefits Summary

### ✅ **Data Quality Assurance**
- **Zero data loss**: Every record tracked (Bronze, Quarantine, or Dead Letter)
- **Automated healing**: Fixed records automatically flow to production
- **Quality SLA**: 24-hour window to fix issues before expiry

### ✅ **Operational Excellence**
- **Full visibility**: Real-time dashboards of data quality metrics
- **Reduced manual work**: No manual CSV uploads or SQL fixes
- **Clear accountability**: Source file tracking for every quarantined record

### ✅ **Auditability & Compliance**
- **Complete audit trail**: Every record's journey documented
- **Historical tracking**: SCD Type 2 preserves full history
- **Regulatory ready**: Timestamp tracking for SOX/GDPR compliance

### ✅ **Scalability & Maintainability**
- **Modular design**: Reusable utility modules
- **Configuration-driven**: Change behavior via YAML, not code
- **Environment parity**: Dev/Prod identical via Asset Bundles
- **Delta Lake foundation**: ACID transactions, time travel, CDF

---

## Next Steps for Client

### Phase 1: Validation (Week 1-2)
- [ ] Deploy to dev environment
- [ ] Run full test cycle with sample data
- [ ] Validate quality rules match business requirements
- [ ] Review quarantine/dead letter workflows

### Phase 2: Integration (Week 3-4)
- [ ] Connect to actual API source
- [ ] Configure production schedules
- [ ] Set up email alerts for failures
- [ ] Train ops team on monitoring queries

### Phase 3: Production (Week 5-6)
- [ ] Deploy to production environment
- [ ] Run parallel with existing system
- [ ] Validate data quality metrics
- [ ] Full cutover with rollback plan

### Phase 4: Optimization (Ongoing)
- [ ] Tune cluster sizes based on load
- [ ] Add custom quality rules
- [ ] Implement business-specific transformations
- [ ] Expand to additional data sources

---

## Technical Stack

| Component | Technology | Purpose |
|-----------|-----------|---------|
| **Compute** | Databricks Spark | Distributed processing |
| **Storage** | Delta Lake | ACID transactions, CDF |
| **Catalog** | Unity Catalog | Data governance |
| **Orchestration** | Databricks Jobs | Scheduled workflows |
| **Streaming** | Delta Live Tables | Silver SCD Type 2 |
| **Deployment** | Asset Bundles | Infrastructure-as-code |
| **Language** | Python (PySpark) | Pipeline logic |
| **Config** | YAML | Environment management |

---

## Support & Documentation

- **Architecture Details**: [PIPELINE_ARCHITECTURE.md](PIPELINE_ARCHITECTURE.md)
- **Data Flow Examples**: [DETAILED_DATA_FLOW.md](DETAILED_DATA_FLOW.md)
- **Table Schemas**: [TABLE_SCHEMAS.md](TABLE_SCHEMAS.md)
- **Testing Guide**: [run_complete_test.py](run_complete_test.py)

---

## Questions for Client

1. **Data Quality Rules**: Are the current validation rules (no nulls in id/name/age) complete, or do you need additional business rules?

2. **Quarantine SLA**: Is 24 hours the right expiry window, or should it be configurable per data source?

3. **Alerting**: Who should receive alerts for quarantine threshold breaches or job failures?

4. **Integration**: What is the actual API endpoint/format for the data source?

5. **Scaling**: What's the expected data volume (records/day) for sizing clusters?

6. **Security**: Are there PII/PHI concerns requiring masking or encryption?

---

**Document Version**: 1.0
**Last Updated**: Reference implementation
