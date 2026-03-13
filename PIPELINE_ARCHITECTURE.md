# Pipeline Architecture

## Overview

This pipeline implements a **three-stage data processing architecture**:

1. **Full/Partial Load Processes**: Simple ingestion from API to Bronze
2. **Quality Check Process**: Validates Landing Zone data and routes to Bronze/Quarantine
3. **Quarantine Maintenance**: Background process for expired records and monitoring

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────┐
│                          API SOURCE (Large)                          │
│                    (Full Load OR Incremental Only)                   │
└───────────────────────────────┬─────────────────────────────────────┘
                                │
                ┌───────────────┴───────────────┐
                │                               │
                ▼                               ▼
    ┌───────────────────────┐       ┌───────────────────────┐
    │   FULL LOAD VOLUME    │       │  PARTIAL LOAD VOLUME  │
    │  /Volumes/.../full/   │       │ /Volumes/.../partial/ │
    │   (Timestamped)       │       │   (Timestamped)       │
    └───────────┬───────────┘       └───────────┬───────────┘
                │                               │
                └───────────────┬───────────────┘
                                │
                ┌───────────────▼────────────────┐
                │    LANDING ZONE (Parquet)      │
                │  Latest timestamped folder     │
                └───────────────┬────────────────┘
                                │
                ┌───────────────▼────────────────┐
                │  QUALITY CHECK PROCESS         │
                │  (LandingZoneQualityCheck.py)  │
                │  Runs AFTER Full/Partial loads │
                └───────────────┬────────────────┘
                                │
                ┌───────────────┴────────────────┐
                │                                │
                ▼                                ▼
    ┌───────────────────────┐       ┌───────────────────────┐
    │   BRONZE TABLE        │       │  QUARANTINE TABLE     │
    │  (Clean Data Only)    │       │  (Bad Data + Stack)   │
    │  CDF Enabled          │       │  Status: ACTIVE       │
    └───────────┬───────────┘       └───────────┬───────────┘
                │                               │
                │          ┌────────────────────┘
                │          │ Fixed Data
                │          │ (No more nulls)
                │          └────────────────────┐
                │                               │
                ▼                               ▼
    ┌───────────────────────┐       ┌───────────────────────┐
    │   SILVER TABLE        │       │  DEAD LETTER TABLE    │
    │  (SCD Type 2)         │       │  (Expired Records)    │
    │  Historical Tracking  │       │  Status: EXPIRED      │
    └───────────────────────┘       └───────────────────────┘
```

---

## Stage 1: Ingestion Processes

### 1A. Full Load (FullLandingZoneBronze.py)

**Purpose**: Capture deleted records from API source

**Process**:
```
Landing Zone (Full) → Bronze Table (OVERWRITE)
```

**Key Features**:
- **Mode**: OVERWRITE
- **When to use**: When API needs to identify deleted records
- **No quality checks** - pure ingestion
- **CDF Enabled**: Change Data Feed for downstream SCD Type 2

**Code Flow**:
```python
# 1. Read latest timestamped folder from full volume
latest_folder = max(folders)
bronze_df = spark.read.parquet(latest_path)

# 2. Overwrite Bronze table
bronze_df.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.{bronze_table}")
```

---

### 1B. Partial Load (PartialLandingZoneBronze.py)

**Purpose**: Efficient incremental updates from API source

**Process**:
```
Landing Zone (Partial) → Bronze Table (MERGE/UPSERT)
```

**Key Features**:
- **Mode**: MERGE (UPSERT on `id`)
- **When to use**: For incremental/update records from large API
- **No quality checks** - pure ingestion
- **Efficient**: Only processes changed records

**Code Flow**:
```python
# 1. Read latest timestamped folder from partial volume
latest_folder = max(folders)
partial_df = spark.read.parquet(latest_path)

# 2. Merge into Bronze table
delta_table.merge(partial_df, "bronze.id = partial.id")
    .whenMatchedUpdateAll()
    .whenNotMatchedInsertAll()
    .execute()
```

---

## Stage 2: Quality Check Process

### 2A. Landing Zone Quality Check (LandingZoneQualityCheck.py)

**Purpose**: Validate data quality and route to appropriate destinations

**Trigger**: Runs AFTER Full/Partial loads complete

**Process**:
```
Landing Zone → Quality Validation → Route:
  ✓ Clean Data → Bronze (APPEND)
  ✗ Bad Data → Quarantine (APPEND)
  ✓ Fixed Quarantine Data → Bronze (APPEND)
```

**Key Features**:
- Reads from **Landing Zone** (latest folders)
- Validates against required columns (no nulls)
- Routes clean records to Bronze
- Routes bad records to Quarantine
- **ID Stacking**: Any update to quarantined ID goes to Quarantine (even if clean)
- Detects fixed records in Quarantine and releases to Bronze
- Processes expired records to Dead Letter

**Code Flow**:
```python
# 1. Get quarantined IDs
quarantined_ids = [row.id for row in quarantine_table
                   where status='ACTIVE']

# 2. Validate Landing Zone data
validated_df = validate_record(landing_zone_df)

# 3. Route based on quality AND quarantine status
clean_df = validated_df.filter(
    ~has_any_null &
    ~id.isin(quarantined_ids)  # Not currently quarantined
)

quarantine_df = validated_df.filter(
    has_any_null |
    id.isin(quarantined_ids)  # Stack updates for quarantined IDs
)

# 4. Write to destinations
clean_df → Bronze (append)
quarantine_df → Quarantine (append)

# 5. Check for fixed records
fixed_records = quarantine_table.filter(
    status='ACTIVE' &
    all_columns_not_null
)

fixed_records → Bronze (append)
update status to 'RELEASED'
```

**Scheduling**:
```python
# Recommended schedule
- Run after each Full Load completes
- Run after each Partial Load completes
- Can be orchestrated via Databricks Jobs workflow
```

---

## Stage 3: Quarantine Maintenance

### 3A. Quarantine Processor (QuarantineProcessor.py)

**Purpose**: Background maintenance for quarantine table

**Trigger**: Scheduled independently (e.g., hourly/daily)

**Process**:
```
Quarantine Table → Check Status → Route:
  ✓ Fixed → Bronze (RELEASED)
  ⏰ Expired → Dead Letter (EXPIRED)
```

**Key Features**:
- Monitors quarantine table health
- Processes expired records (beyond 24 hours)
- Moves expired records to Dead Letter table
- Provides detailed health reports

**Code Flow**:
```python
# 1. Find expired records
expired_records = quarantine_table.filter(
    status='ACTIVE' &
    quarantine_expiry < current_timestamp()
)

# 2. Move to Dead Letter
expired_records → Dead Letter Table
update status to 'EXPIRED'

# 3. Health reporting
- Count by status (ACTIVE, FIXED, RELEASED, EXPIRED)
- Records expiring soon (next 6 hours)
- Oldest active quarantined records
```

---

## Data Flow Examples

### Example 1: Clean Record Flow
```
1. API → Landing Zone: {id: 1, name: "Alice", age: 30}
2. Full/Partial Load → Bronze: Direct write
3. Quality Check: Validates ✓ → Already in Bronze
4. Bronze → Silver: SCD Type 2 tracking
```

### Example 2: Bad Record Flow
```
1. API → Landing Zone: {id: 2, name: null, age: 25}
2. Full/Partial Load → Bronze: Direct write (no validation)
3. Quality Check: Validates ✗ → Quarantine
   - Status: ACTIVE
   - Reason: "null_name"
   - Expiry: +24 hours
4. WAIT for fix...
5. Fixed data arrives: {id: 2, name: "Bob", age: 25}
6. Quality Check: Detects fix → Bronze
   - Updates quarantine status: RELEASED
```

### Example 3: Stacked Updates Flow
```
1. API → Landing Zone: {id: 3, name: null, age: 27}
2. Quality Check: Validates ✗ → Quarantine (v1)
   - Status: ACTIVE

3. API → Landing Zone: {id: 3, name: null, age: 28}  ← Update (still bad)
4. Quality Check: ID quarantined → Quarantine (v2)
   - Status: ACTIVE (stacked)

5. API → Landing Zone: {id: 3, name: "Charlie", age: 29}  ← Fixed!
6. Quality Check: ID quarantined BUT now valid → Bronze
   - All versions marked RELEASED
   - Latest version {id: 3, name: "Charlie", age: 29} → Bronze
```

### Example 4: Expired Record Flow
```
1. Record in Quarantine for 24 hours (no fix)
2. Quarantine Processor: Checks expiry
3. Expired → Dead Letter Table
4. Status updated: EXPIRED
5. Manual review/intervention needed
```

---

## Key Design Decisions

### Why Separate Processes?

1. **Performance**: Full/Partial loads are fast (no quality overhead)
2. **Scalability**: Quality checks can run independently
3. **Flexibility**: Quality rules can be updated without changing ingestion
4. **Monitoring**: Clear separation of concerns for debugging

### Why Landing Zone Validation?

- **Source of truth**: Landing Zone contains raw API data
- **No bad data in Bronze**: Bronze only contains validated records OR records from Full/Partial loads before quality check runs
- **Auditability**: Can re-run quality checks on same Landing Zone data

### Why ID Stacking?

- **Consistency**: Once an ID has quality issues, ALL updates wait in quarantine
- **Prevents partial updates**: Ensures Bronze doesn't have mixed good/bad versions
- **Clear resolution**: Single fix releases all stacked versions

---

## Configuration (config.yaml)

```yaml
Catalog: "your_catalog"
Schema: "dp_ingestion_one_flow"

Vol_Partial: "parquet_partial"
Vol_Full: "parquet_full"

Bronze_table: "bronze_table"
Quarantine_table: "quarantine_table"
Dead_letter_table: "dead_letter_table"
Silver_table: "silver_table"

# Quality Control Settings
Quarantine_Hours: 24  # Time to fix before expiry
Required_Columns: ["id", "name", "age"]  # No nulls allowed
```

---

## Notebook Execution Order

### Manual Execution
```bash
1. Run Full Load OR Partial Load
   - FullLandingZoneBronze.py (when deletes need to be captured)
   - PartialLandingZoneBronze.py (for incremental updates)

2. Run Quality Check (AFTER load completes)
   - LandingZoneQualityCheck.py

3. Run Silver Transformation (Declarative Pipeline - continuous)
   - SilverTableDP.py

4. Run Quarantine Maintenance (scheduled - hourly/daily)
   - QuarantineProcessor.py
```

### Automated Orchestration (Databricks Jobs)

```python
Job 1: Full Load Pipeline
├─ Task 1: FullLandingZoneBronze.py
└─ Task 2: LandingZoneQualityCheck.py (depends on Task 1)

Job 2: Partial Load Pipeline
├─ Task 1: PartialLandingZoneBronze.py
└─ Task 2: LandingZoneQualityCheck.py (depends on Task 1)

Job 3: Silver Transformation
└─ Task: SilverTableDP.py (Declarative Pipeline - continuous)

Job 4: Quarantine Maintenance
└─ Task: QuarantineProcessor.py (scheduled - every 6 hours)
```

---

## Table Summary

| Table | Purpose | Data Source | Update Mode |
|-------|---------|-------------|-------------|
| **Bronze** | Clean validated data | Landing Zone (via Quality Check) | Full: OVERWRITE<br>Partial: MERGE<br>Quality: APPEND |
| **Quarantine** | Records with quality issues | Landing Zone (via Quality Check) | APPEND (stacking) |
| **Dead Letter** | Expired unfixable records | Quarantine (via Processor) | APPEND |
| **Silver** | SCD Type 2 historical data | Bronze (via Declarative Pipeline) | CDC with auto_cdc_flow |

---

## Monitoring Queries

### Check Quality Pipeline Health
```sql
-- Bronze record count
SELECT COUNT(*) as bronze_count
FROM your_catalog.your_schema.bronze_table;

-- Quarantine status breakdown
SELECT quarantine_status, COUNT(*) as count, COUNT(DISTINCT id) as unique_ids
FROM your_catalog.your_schema.quarantine_table
GROUP BY quarantine_status;

-- Active quarantine records expiring soon
SELECT id, quarantine_reason,
       ROUND((unix_timestamp(quarantine_expiry) - unix_timestamp(current_timestamp())) / 3600, 2) as hours_remaining
FROM your_catalog.your_schema.quarantine_table
WHERE quarantine_status = 'ACTIVE'
  AND quarantine_expiry < current_timestamp() + INTERVAL 6 HOURS
ORDER BY quarantine_expiry;
```

---

## Next Steps

1. **Test the Pipeline**: Run `TestPipelineWithQuarantine.py`
2. **Schedule Jobs**: Create Databricks Jobs for automated orchestration
3. **Monitor**: Set up alerts for quarantine threshold breaches
4. **Tune**: Adjust `Quarantine_Hours` based on fix SLA requirements
