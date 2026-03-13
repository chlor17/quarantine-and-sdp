# Pipeline Table Schemas and Sample Data

## Architecture Overview

```
Landing Zone (Volumes) → Quality Check → Bronze/Quarantine → Silver (SCD Type 2)
                                              ↓
                                         Dead Letter
```

---

## 1. Landing Zone (Volumes)

**Location**: `/Volumes/{catalog}/{schema}/{volume_name}/{volume_name}_{timestamp}/`

**Format**: Parquet files with timestamped folders

**Schema**:
```
id      INT
name    STRING
age     INT
```

**Sample Data** (parquet_full_20260303120000/):
```
+----+---------+-----+
| id | name    | age |
+----+---------+-----+
| 1  | Alice   | 30  |
| 2  | Bob     | NULL|  ← Quality issue
| 3  | NULL    | 25  |  ← Quality issue
| 4  | Charlie | 35  |
| 5  | Diana   | 28  |
+----+---------+-----+
```

---

## 2. Bronze Table

**Full Name**: `{catalog}.{schema}.bronze_table`

**Purpose**: Validated, clean data ready for consumption

**Schema**:
```sql
CREATE TABLE bronze_table (
  id      INT,
  name    STRING,
  age     INT
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'comment' = 'Bronze table with validated data'
)
```

**Sample Data** (after quality check):
```
+----+---------+-----+
| id | name    | age |
+----+---------+-----+
| 1  | Alice   | 30  |
| 4  | Charlie | 35  |
| 5  | Diana   | 28  |
| 7  | Eve     | 32  |
+----+---------+-----+
```

**Change Data Feed** (enabled for SCD Type 2):
```
+----+---------+-----+--------------+-------------------+----------------+
| id | name    | age | _change_type | _commit_timestamp | _commit_version|
+----+---------+-----+--------------+-------------------+----------------+
| 1  | Alice   | 30  | insert       | 2026-03-03 12:00  | 1              |
| 1  | Alice   | 31  | update_pre   | 2026-03-03 12:05  | 2              |
| 1  | Alice   | 31  | update_post  | 2026-03-03 12:05  | 2              |
+----+---------+-----+--------------+-------------------+----------------+
```

---

## 3. Quarantine Table

**Full Name**: `{catalog}.{schema}.quarantine_table`

**Purpose**: Holds records with quality issues for review and correction

**Schema**:
```sql
CREATE TABLE quarantine_table (
  -- Original data columns
  id                    INT,
  name                  STRING,
  age                   INT,

  -- Quarantine metadata
  quarantine_timestamp  TIMESTAMP,      -- When record was quarantined
  quarantine_expiry     TIMESTAMP,      -- When quarantine expires (timestamp + 24h)
  quarantine_status     STRING,         -- ACTIVE, FIXED, RELEASED, EXPIRED
  validation_errors     ARRAY<STRING>,  -- List of validation issues
  quarantine_reason     STRING,         -- Human-readable reason
  fix_attempts          INT,            -- Number of fix attempts

  -- Audit trail
  source_file           STRING,         -- Source parquet file path
  ingestion_timestamp   TIMESTAMP       -- When ingested
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'comment' = 'Quarantine table for data quality issues'
)
```

**Sample Data**:
```
+----+------+------+---------------------+---------------------+------------------+-------------------+-------------------+--------------+------------------------------------------+---------------------+
| id | name | age  | quarantine_timestamp| quarantine_expiry   | quarantine_status| validation_errors | quarantine_reason | fix_attempts | source_file                              | ingestion_timestamp |
+----+------+------+---------------------+---------------------+------------------+-------------------+-------------------+--------------+------------------------------------------+---------------------+
| 2  | Bob  | NULL | 2026-03-03 12:00:00 | 2026-03-04 12:00:00 | ACTIVE           | [null_age]        | null_age          | 0            | /Volumes/.../parquet_full_20260303120000 | 2026-03-03 12:00:00 |
| 3  | NULL | 25   | 2026-03-03 12:00:00 | 2026-03-04 12:00:00 | ACTIVE           | [null_name]       | null_name         | 0            | /Volumes/.../parquet_full_20260303120000 | 2026-03-03 12:00:00 |
| 3  | NULL | 26   | 2026-03-03 12:05:00 | 2026-03-04 12:05:00 | ACTIVE           | [null_name]       | null_name         | 0            | /Volumes/.../parquet_partial_2026...     | 2026-03-03 12:05:00 |
| 6  | NULL | NULL | 2026-03-03 12:00:00 | 2026-03-04 12:00:00 | ACTIVE           | [null_name, null_age] | null_name, null_age | 0        | /Volumes/.../parquet_full_20260303120000 | 2026-03-03 12:00:00 |
| 11 | Grace| NULL | 2026-03-03 12:05:00 | 2026-03-04 12:05:00 | ACTIVE           | [null_age]        | null_age          | 0            | /Volumes/.../parquet_partial_2026...     | 2026-03-03 12:05:00 |
+----+------+------+---------------------+---------------------+------------------+-------------------+-------------------+--------------+------------------------------------------+---------------------+
```

**Quarantine Status Flow**:
```
ACTIVE → FIXED → RELEASED  (successful fix)
   ↓
EXPIRED → Dead Letter      (timeout without fix)
```

**Example: Stacked Records for Same ID**:
```
+----+-------------+-----+---------------------+------------------+-------------------+
| id | name        | age | quarantine_timestamp| quarantine_status| quarantine_reason |
+----+-------------+-----+---------------------+------------------+-------------------+
| 3  | NULL        | 25  | 2026-03-03 12:00:00 | ACTIVE           | null_name         |  ← Original
| 3  | NULL        | 26  | 2026-03-03 12:05:00 | ACTIVE           | null_name         |  ← Update 1 (stacked)
| 3  | Fixed Name  | 27  | 2026-03-03 12:10:00 | FIXED            | null_name         |  ← Update 2 (fixed!)
+----+-------------+-----+---------------------+------------------+-------------------+
```

**Querying Active Quarantine**:
```sql
SELECT
  id,
  name,
  age,
  quarantine_reason,
  ROUND((unix_timestamp(quarantine_expiry) - unix_timestamp(current_timestamp())) / 3600, 2) as hours_remaining
FROM your_catalog.your_schema.quarantine_table
WHERE quarantine_status = 'ACTIVE'
ORDER BY quarantine_expiry ASC
```

---

## 4. Dead Letter Table

**Full Name**: `{catalog}.{schema}.dead_letter_table`

**Purpose**: Permanent storage for records that couldn't be fixed within quarantine period

**Schema**:
```sql
CREATE TABLE dead_letter_table (
  -- Original data columns
  id                    INT,
  name                  STRING,
  age                   INT,

  -- Quarantine metadata (preserved from quarantine)
  quarantine_timestamp  TIMESTAMP,
  quarantine_expiry     TIMESTAMP,
  quarantine_status     STRING,
  validation_errors     ARRAY<STRING>,
  quarantine_reason     STRING,
  fix_attempts          INT,

  -- Audit trail
  source_file           STRING,
  ingestion_timestamp   TIMESTAMP,
  moved_to_dead_letter  TIMESTAMP      -- When moved from quarantine
)
TBLPROPERTIES (
  'comment' = 'Dead letter table for expired/unfixable records'
)
```

**Sample Data**:
```
+----+------+------+---------------------+---------------------+------------------+-------------------+-------------------+--------------+---------------------+----------------------+
| id | name | age  | quarantine_timestamp| quarantine_expiry   | quarantine_status| validation_errors | quarantine_reason | fix_attempts | ingestion_timestamp | moved_to_dead_letter |
+----+------+------+---------------------+---------------------+------------------+-------------------+-------------------+--------------+---------------------+----------------------+
| 6  | NULL | NULL | 2026-03-03 12:00:00 | 2026-03-04 12:00:00 | EXPIRED          | [null_name, null_age] | null_name, null_age | 0        | 2026-03-03 12:00:00 | 2026-03-04 12:05:00  |
+----+------+------+---------------------+---------------------+------------------+-------------------+-------------------+--------------+---------------------+----------------------+
```

**Use Cases**:
- Manual review of problematic records
- Data quality reporting
- Source system feedback
- Recovery/reprocessing

---

## 5. Silver Table (SCD Type 2)

**Full Name**: `{catalog}.{schema}.silver_table`

**Purpose**: Business-ready data with full historical tracking

**Schema**:
```sql
-- Created by Declarative Pipeline with SCD Type 2
CREATE STREAMING TABLE silver (
  id            INT,
  name          STRING,
  age           INT,

  -- SCD Type 2 columns (auto-generated)
  __START_AT    TIMESTAMP,      -- When this version became active
  __END_AT      TIMESTAMP,      -- When this version became inactive (NULL = current)
  __CURRENT     BOOLEAN         -- TRUE if this is the current version
)
COMMENT 'Silver layer with SCD Type 2 tracking for full loads'
```

**Sample Data** (with history):
```
+----+---------------+-----+---------------------+---------------------+-----------+
| id | name          | age | __START_AT          | __END_AT            | __CURRENT |
+----+---------------+-----+---------------------+---------------------+-----------+
| 1  | Alice         | 30  | 2026-03-03 12:00:00 | 2026-03-03 12:05:00 | false     |  ← Historical
| 1  | Alice Updated | 31  | 2026-03-03 12:05:00 | NULL                | true      |  ← Current
| 2  | Bob Fixed     | 40  | 2026-03-03 12:05:00 | NULL                | true      |  ← Released from quarantine
| 4  | Charlie       | 35  | 2026-03-03 12:00:00 | NULL                | true      |
| 5  | Diana         | 28  | 2026-03-03 12:00:00 | NULL                | true      |
| 7  | Eve           | 32  | 2026-03-03 12:00:00 | NULL                | true      |
| 10 | Frank         | 45  | 2026-03-03 12:05:00 | NULL                | true      |
+----+---------------+-----+---------------------+---------------------+-----------+
```

**Querying Current State**:
```sql
SELECT id, name, age
FROM your_catalog.your_schema.silver_table
WHERE __CURRENT = true
```

**Querying History for Specific ID**:
```sql
SELECT
  id,
  name,
  age,
  __START_AT,
  __END_AT,
  __CURRENT,
  DATEDIFF(__END_AT, __START_AT) as days_active
FROM your_catalog.your_schema.silver_table
WHERE id = 1
ORDER BY __START_AT
```

**Time Travel Query** (as of specific date):
```sql
SELECT id, name, age
FROM your_catalog.your_schema.silver_table
WHERE id = 1
  AND __START_AT <= '2026-03-03 12:03:00'
  AND (__END_AT > '2026-03-03 12:03:00' OR __END_AT IS NULL)
```

---

## Complete Data Flow Example

### Scenario: Record with ID=2 journey

**Step 1: Initial Load (Full)**
```
Landing Zone:
+----+------+------+
| id | name | age  |
+----+------+------+
| 2  | Bob  | NULL |  ← Missing age
+----+------+------+

↓ Quality Check

Quarantine Table:
+----+------+------+---------------------+------------------+------------------+
| id | name | age  | quarantine_timestamp| quarantine_status| validation_errors|
+----+------+------+---------------------+------------------+------------------+
| 2  | Bob  | NULL | 2026-03-03 12:00:00 | ACTIVE           | [null_age]       |
+----+------+------+---------------------+------------------+------------------+

Bronze Table: (empty for id=2)
```

**Step 2: Partial Load (Fix Provided)**
```
Landing Zone:
+----+-----------+-----+
| id | name      | age |
+----+-----------+-----+
| 2  | Bob Fixed | 40  |  ← Age provided!
+----+-----------+-----+

↓ Quality Check (PASS!)

Quarantine Table (updated):
+----+-----------+------+---------------------+------------------+------------------+
| id | name      | age  | quarantine_timestamp| quarantine_status| validation_errors|
+----+-----------+------+---------------------+------------------+------------------+
| 2  | Bob       | NULL | 2026-03-03 12:00:00 | FIXED            | [null_age]       |
+----+-----------+------+---------------------+------------------+------------------+

Bronze Table (MERGE):
+----+-----------+-----+
| id | name      | age |
+----+-----------+-----+
| 2  | Bob Fixed | 40  |  ← Merged!
+----+-----------+-----+
```

**Step 3: QuarantineProcessor (Release)**
```
Quarantine Table (updated):
+----+-----------+------+---------------------+------------------+------------------+
| id | name      | age  | quarantine_timestamp| quarantine_status| validation_errors|
+----+-----------+------+---------------------+------------------+------------------+
| 2  | Bob       | NULL | 2026-03-03 12:00:00 | RELEASED         | [null_age]       |
+----+-----------+------+---------------------+------------------+------------------+
```

**Step 4: Silver Layer (SCD Type 2)**
```
Silver Table:
+----+-----------+-----+---------------------+----------+-----------+
| id | name      | age | __START_AT          | __END_AT | __CURRENT |
+----+-----------+-----+---------------------+----------+-----------+
| 2  | Bob Fixed | 40  | 2026-03-03 12:05:00 | NULL     | true      |
+----+-----------+-----+---------------------+----------+-----------+
```

---

## Monitoring Queries

### 1. Quarantine Health Check
```sql
SELECT
  quarantine_status,
  COUNT(*) as total_records,
  COUNT(DISTINCT id) as unique_ids
FROM your_catalog.your_schema.quarantine_table
GROUP BY quarantine_status
ORDER BY total_records DESC
```

### 2. Top Quality Issues
```sql
SELECT
  quarantine_reason,
  COUNT(*) as count,
  COLLECT_LIST(DISTINCT id) as affected_ids
FROM your_catalog.your_schema.quarantine_table
WHERE quarantine_status = 'ACTIVE'
GROUP BY quarantine_reason
ORDER BY count DESC
```

### 3. Records Approaching Expiry
```sql
SELECT
  id,
  name,
  age,
  quarantine_reason,
  ROUND((unix_timestamp(quarantine_expiry) - unix_timestamp(current_timestamp())) / 3600, 2) as hours_remaining
FROM your_catalog.your_schema.quarantine_table
WHERE quarantine_status = 'ACTIVE'
  AND quarantine_expiry > current_timestamp()
  AND quarantine_expiry < current_timestamp() + INTERVAL 6 HOURS
ORDER BY hours_remaining
```

### 4. Stacked Records (Same ID)
```sql
SELECT
  id,
  COUNT(*) as version_count,
  COLLECT_LIST(STRUCT(quarantine_timestamp, quarantine_status, validation_errors)) as versions
FROM your_catalog.your_schema.quarantine_table
GROUP BY id
HAVING COUNT(*) > 1
ORDER BY version_count DESC
```

### 5. Data Quality Metrics
```sql
WITH metrics AS (
  SELECT
    (SELECT COUNT(*) FROM your_catalog.your_schema.bronze_table) as bronze_count,
    (SELECT COUNT(*) FROM your_catalog.your_schema.quarantine_table WHERE quarantine_status = 'ACTIVE') as active_quarantine,
    (SELECT COUNT(*) FROM your_catalog.your_schema.dead_letter_table) as dead_letter_count
)
SELECT
  bronze_count,
  active_quarantine,
  dead_letter_count,
  bronze_count + active_quarantine + dead_letter_count as total_processed,
  ROUND(bronze_count * 100.0 / (bronze_count + active_quarantine + dead_letter_count), 2) as quality_rate_pct
FROM metrics
```

---

## Table Relationships

```
┌─────────────────────────────────────────────────────────────────────┐
│                        Landing Zone (Volumes)                       │
│  Raw parquet files with timestamp folders                           │
└────────────────────┬────────────────────────────────────────────────┘
                     │
                     ▼
        ┌───────────────────────────┐
        │   Quality Validation      │
        │   - Null checks           │
        │   - ID quarantine check   │
        └────────┬──────────┬───────┘
                 │          │
          PASS   │          │  FAIL
                 │          │
                 ▼          ▼
    ┌───────────────┐   ┌──────────────────┐
    │ Bronze Table  │   │ Quarantine Table │
    │ (Clean Data)  │   │ (24hr hold)      │
    │               │   │                  │
    │ - id          │   │ - id, name, age  │
    │ - name        │   │ - status         │
    │ - age         │   │ - errors         │
    │ + CDF enabled │   │ - timestamps     │
    └───────┬───────┘   └────────┬─────────┘
            │                    │
            │           FIXED/   │  EXPIRED
            │           RELEASED │
            │                    │          ┌──────────────────┐
            │                    └─────────→│ Dead Letter Table│
            │                               │ (Permanent)      │
            │                               └──────────────────┘
            │
            ▼
    ┌───────────────────────────┐
    │   Silver Table            │
    │   (SCD Type 2)            │
    │                           │
    │ - id, name, age           │
    │ - __START_AT              │
    │ - __END_AT                │
    │ - __CURRENT               │
    │                           │
    │ Full history preserved    │
    └───────────────────────────┘
```
