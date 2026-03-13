# Detailed Data Flow - Step by Step

This document shows **exactly** what data appears in each table at each step of the pipeline.

---

## Test Scenario Overview

We'll trace 8 records through the system:
- **4 records** in Full Load (2 clean, 2 bad)
- **3 records** in Partial Load (1 update, 1 fix, 1 new bad)
- **1 record** that expires in Quarantine

---

## STEP 1: Full Load - Landing Zone

**File Created:** `/Volumes/your_catalog/your_schema/parquet_full/20260306_143022/`

### Data in Landing Zone
```
| id | name    | age  |
|----|---------|------|
| 1  | Alice   | 30   |  ← Clean
| 2  | Bob     | NULL |  ← Bad: missing age
| 3  | NULL    | 25   |  ← Bad: missing name
| 4  | Charlie | 35   |  ← Clean
```

**Status:**
- ✅ Clean: 2 records (id=1, id=4)
- ❌ Bad: 2 records (id=2, id=3)

---

## STEP 2: Full Load Process (FullLandingZoneBronze.py)

### Action: Write to Bronze (OVERWRITE mode)

**Bronze Table** (After Full Load):
```
| id | name    | age  |
|----|---------|------|
| 1  | Alice   | 30   |
| 2  | Bob     | NULL |  ← BAD DATA in Bronze!
| 3  | NULL    | 25   |  ← BAD DATA in Bronze!
| 4  | Charlie | 35   |
```

**Quarantine Table**: *(empty - no quality checks yet)*

**Notes:**
- Bronze contains ALL 4 records, including bad ones
- No quality validation at this stage
- This is intentional - Full/Partial loads are separate from Quality Checks

---

## STEP 3: Quality Check Process (LandingZoneQualityCheck.py)

### Action: Read Landing Zone, Validate, Route Records

**Step 3A: Validation**

Reading from Landing Zone, applying quality rules:

```
| id | name    | age  | validation_errors  | has_any_null |
|----|---------|------|--------------------|--------------|
| 1  | Alice   | 30   | []                 | false        |
| 2  | Bob     | NULL | ["null_age"]       | true         |
| 3  | NULL    | 25   | ["null_name"]      | true         |
| 4  | Charlie | 35   | []                 | false        |
```

**Step 3B: Split into Clean vs Quarantine**

**Clean Records** (has_any_null = false):
```
| id | name    | age  |
|----|---------|------|
| 1  | Alice   | 30   |
| 4  | Charlie | 35   |
```

**Quarantine Records** (has_any_null = true):
```
| id | name | age  | quarantine_reason | quarantine_status | quarantine_expiry        |
|----|------|------|-------------------|-------------------|--------------------------|
| 2  | Bob  | NULL | null_age          | ACTIVE            | 2026-03-07 14:30:22     |
| 3  | NULL | 25   | null_name         | ACTIVE            | 2026-03-07 14:30:22     |
```

**Step 3C: Remove Bad Records from Bronze**

Execute: `DELETE FROM bronze_table`

**Bronze Table** (After DELETE):
```
(empty)
```

**Step 3D: Write Clean Records to Bronze**

**Bronze Table** (After Quality Check):
```
| id | name    | age  |
|----|---------|------|
| 1  | Alice   | 30   |  ← From Landing Zone, validated clean
| 4  | Charlie | 35   |  ← From Landing Zone, validated clean
```

**Quarantine Table** (After Quality Check):
```
| id | name | age  | quarantine_timestamp | quarantine_expiry    | quarantine_status | validation_errors | quarantine_reason | fix_attempts | source_file                                  |
|----|------|------|---------------------|----------------------|-------------------|-------------------|-------------------|--------------|---------------------------------------------|
| 2  | Bob  | NULL | 2026-03-06 14:30:22 | 2026-03-07 14:30:22 | ACTIVE            | ["null_age"]      | null_age          | 0            | /Volumes/.../parquet_full/20260306_143022   |
| 3  | NULL | 25   | 2026-03-06 14:30:22 | 2026-03-07 14:30:22 | ACTIVE            | ["null_name"]     | null_name         | 0            | /Volumes/.../parquet_full/20260306_143022   |
```

**Dead Letter Table**: *(empty)*

---

## STEP 4: Partial Load - Landing Zone

**File Created:** `/Volumes/your_catalog/your_schema/parquet_partial/20260306_150000/`

### Data in Landing Zone (Partial)
```
| id | name         | age  |
|----|--------------|------|
| 1  | Alice Smith  | 31   |  ← Update to existing clean record
| 2  | Bob Johnson  | 40   |  ← FIX for quarantined record (now has age!)
| 9  | George       | NULL |  ← New bad record
```

**Notes:**
- id=1: Update (name changed, age increased)
- id=2: Fix (Bob now has an age value)
- id=9: New record with quality issue

---

## STEP 5: Partial Load Process (PartialLandingZoneBronze.py)

### Action: MERGE into Bronze

**Before Merge - Bronze Table:**
```
| id | name    | age  |
|----|---------|------|
| 1  | Alice   | 30   |
| 4  | Charlie | 35   |
```

**After Merge - Bronze Table:**
```
| id | name         | age  |
|----|--------------|------|
| 1  | Alice Smith  | 31   |  ← UPDATED (matched on id)
| 2  | Bob Johnson  | 40   |  ← INSERTED (new)
| 4  | Charlie      | 35   |  ← Unchanged
| 9  | George       | NULL |  ← INSERTED (new) - BAD DATA!
```

**Notes:**
- Bronze now has BAD data again (id=2 shouldn't be here yet, id=9 is bad)
- This is temporary - Quality Check will clean it up

---

## STEP 6: Quality Check Process (LandingZoneQualityCheck.py) - Second Run

### Action: Read Latest Partial Landing Zone, Validate, Route

**Step 6A: Get Currently Quarantined IDs**

Query quarantine table for active records:
```sql
SELECT id FROM quarantine_table WHERE quarantine_status = 'ACTIVE'
```

Result: `[2, 3]` ← These IDs are currently quarantined

**Step 6B: Validation**

Reading from Landing Zone (Partial), applying quality rules:

```
| id | name         | age  | validation_errors | has_any_null | id_is_quarantined |
|----|--------------|------|-------------------|--------------|-------------------|
| 1  | Alice Smith  | 31   | []                | false        | false             |
| 2  | Bob Johnson  | 40   | []                | false        | true (id=2 was)   |
| 9  | George       | NULL | ["null_age"]      | true         | false             |
```

**Step 6C: Split Based on Quality AND Quarantine Status**

**Clean Records** (has_any_null = false AND id NOT quarantined):
```
| id | name         | age  |
|----|--------------|------|
| 1  | Alice Smith  | 31   |
```

**Quarantine Records** (has_any_null = true OR id IS quarantined):
```
| id | name        | age  | quarantine_reason            | why_quarantined             |
|----|-------------|------|------------------------------|----------------------------|
| 2  | Bob Johnson | 40   | (inherited from previous)    | ID was quarantined, stacking|
| 9  | George      | NULL | null_age                     | Has null value              |
```

**Key Insight:** id=2 goes to Quarantine even though it's now clean! This is **ID Stacking**.

**Step 6D: Remove Bad Records from Bronze**

Bad IDs to remove: `[2, 9]`

```sql
DELETE FROM bronze_table WHERE id IN (2, 9)
```

**Bronze Table** (After DELETE):
```
| id | name         | age  |
|----|--------------|------|
| 1  | Alice Smith  | 31   |
| 4  | Charlie      | 35   |
```

**Step 6E: Merge Clean Records to Bronze**

**Bronze Table** (After Merge):
```
| id | name         | age  |
|----|--------------|------|
| 1  | Alice Smith  | 31   |  ← UPDATED from partial load
| 4  | Charlie      | 35   |  ← Unchanged
```

**Step 6F: Append to Quarantine**

**Quarantine Table** (After Append):
```
| id | name        | age  | quarantine_timestamp | quarantine_status | quarantine_reason | fix_attempts | notes                    |
|----|-------------|------|---------------------|-------------------|-------------------|--------------|--------------------------|
| 2  | Bob         | NULL | 2026-03-06 14:30:22 | ACTIVE            | null_age          | 0            | ← Original bad version   |
| 3  | NULL        | 25   | 2026-03-06 14:30:22 | ACTIVE            | null_name         | 0            | ← Still quarantined      |
| 2  | Bob Johnson | 40   | 2026-03-06 15:00:00 | ACTIVE            | null_age          | 0            | ← STACKED (id=2 again!)  |
| 9  | George      | NULL | 2026-03-06 15:00:00 | ACTIVE            | null_age          | 0            | ← New bad record         |
```

**Key Point:** Notice id=2 appears TWICE in Quarantine - the old bad version and the new good version (stacking).

---

## STEP 7: Process Fixed Records from Quarantine

### Action: Detect and Release Fixed Records

**Step 7A: Find Fixed Records**

Query quarantine for records that are now valid:

```sql
SELECT * FROM quarantine_table
WHERE quarantine_status = 'ACTIVE'
  AND id IS NOT NULL
  AND name IS NOT NULL
  AND age IS NOT NULL
```

**Fixed Records Found:**
```
| id | name        | age  | quarantine_timestamp |
|----|-------------|------|---------------------|
| 2  | Bob Johnson | 40   | 2026-03-06 15:00:00 |  ← This version is complete!
```

**Step 7B: Mark ALL Versions of Fixed IDs as FIXED**

Fixed IDs: `[2]`

```sql
UPDATE quarantine_table
SET quarantine_status = 'FIXED', fix_attempts = fix_attempts + 1
WHERE id IN (2) AND quarantine_status = 'ACTIVE'
```

**Quarantine Table** (After Update):
```
| id | name        | age  | quarantine_timestamp | quarantine_status | fix_attempts | version |
|----|-------------|------|---------------------|-------------------|--------------|---------|
| 2  | Bob         | NULL | 2026-03-06 14:30:22 | FIXED             | 1            | v1      |
| 3  | NULL        | 25   | 2026-03-06 14:30:22 | ACTIVE            | 0            | -       |
| 2  | Bob Johnson | 40   | 2026-03-06 15:00:00 | FIXED             | 1            | v2      |
| 9  | George      | NULL | 2026-03-06 15:00:00 | ACTIVE            | 0            | -       |
```

**Step 7C: Get Latest Fixed Version per ID**

```sql
SELECT id, name, age
FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY id ORDER BY quarantine_timestamp DESC) as rn
  FROM quarantine_table
  WHERE quarantine_status = 'FIXED'
)
WHERE rn = 1
```

**Records to Release:**
```
| id | name        | age  |
|----|-------------|------|
| 2  | Bob Johnson | 40   |  ← Latest version of id=2
```

**Step 7D: Write to Bronze**

**Bronze Table** (After Release):
```
| id | name         | age  |
|----|--------------|------|
| 1  | Alice Smith  | 31   |
| 2  | Bob Johnson  | 40   |  ← RELEASED from Quarantine!
| 4  | Charlie      | 35   |
```

**Step 7E: Mark as RELEASED**

```sql
UPDATE quarantine_table
SET quarantine_status = 'RELEASED'
WHERE quarantine_status = 'FIXED'
```

**Quarantine Table** (Final):
```
| id | name        | age  | quarantine_timestamp | quarantine_status | fix_attempts |
|----|-------------|------|---------------------|-------------------|--------------|
| 2  | Bob         | NULL | 2026-03-06 14:30:22 | RELEASED          | 1            |
| 3  | NULL        | 25   | 2026-03-06 14:30:22 | ACTIVE            | 0            |
| 2  | Bob Johnson | 40   | 2026-03-06 15:00:00 | RELEASED          | 1            |
| 9  | George      | NULL | 2026-03-06 15:00:00 | ACTIVE            | 0            |
```

---

## STEP 8: Process Expired Records (24 Hours Later)

**Current Time:** 2026-03-07 15:00:00 (24+ hours after initial quarantine)

### Action: Move Expired Records to Dead Letter

**Step 8A: Find Expired Records**

```sql
SELECT * FROM quarantine_table
WHERE quarantine_status = 'ACTIVE'
  AND quarantine_expiry < CURRENT_TIMESTAMP()
```

**Expired Records:**
```
| id | name   | age  | quarantine_timestamp | quarantine_expiry    |
|----|--------|------|---------------------|----------------------|
| 3  | NULL   | 25   | 2026-03-06 14:30:22 | 2026-03-07 14:30:22 |
| 9  | George | NULL | 2026-03-06 15:00:00 | 2026-03-07 15:00:00 |
```

**Step 8B: Move to Dead Letter Table**

**Dead Letter Table** (After Move):
```
| id | name   | age  | quarantine_timestamp | quarantine_expiry    | quarantine_status | quarantine_reason | moved_to_dead_letter |
|----|--------|------|---------------------|----------------------|-------------------|-------------------|---------------------|
| 3  | NULL   | 25   | 2026-03-06 14:30:22 | 2026-03-07 14:30:22 | ACTIVE            | null_name         | 2026-03-07 15:00:00 |
| 9  | George | NULL | 2026-03-06 15:00:00 | 2026-03-07 15:00:00 | ACTIVE            | null_age          | 2026-03-07 15:00:00 |
```

**Step 8C: Mark as EXPIRED in Quarantine**

```sql
UPDATE quarantine_table
SET quarantine_status = 'EXPIRED'
WHERE id IN (3, 9) AND quarantine_status = 'ACTIVE'
```

**Quarantine Table** (After Expiry):
```
| id | name        | age  | quarantine_timestamp | quarantine_status | fix_attempts |
|----|-------------|------|---------------------|-------------------|--------------|
| 2  | Bob         | NULL | 2026-03-06 14:30:22 | RELEASED          | 1            |
| 3  | NULL        | 25   | 2026-03-06 14:30:22 | EXPIRED           | 0            |
| 2  | Bob Johnson | 40   | 2026-03-06 15:00:00 | RELEASED          | 1            |
| 9  | George      | NULL | 2026-03-06 15:00:00 | EXPIRED           | 0            |
```

---

## FINAL STATE: All Tables

### Bronze Table (Production Data - Clean Only)
```
| id | name         | age  |
|----|--------------|------|
| 1  | Alice Smith  | 31   |
| 2  | Bob Johnson  | 40   |
| 4  | Charlie      | 35   |
```
**Count:** 3 records (all clean, validated)

---

### Quarantine Table (Historical Record)
```
| id | name        | age  | quarantine_timestamp | quarantine_status | fix_attempts | journey                  |
|----|-------------|------|---------------------|-------------------|--------------|--------------------------|
| 2  | Bob         | NULL | 2026-03-06 14:30:22 | RELEASED          | 1            | Bad → Fixed → Released   |
| 3  | NULL        | 25   | 2026-03-06 14:30:22 | EXPIRED           | 0            | Bad → Expired            |
| 2  | Bob Johnson | 40   | 2026-03-06 15:00:00 | RELEASED          | 1            | Stacked → Released       |
| 9  | George      | NULL | 2026-03-06 15:00:00 | EXPIRED           | 0            | Bad → Expired            |
```
**Count:** 4 records total
- RELEASED: 2 (both versions of id=2)
- EXPIRED: 2 (id=3, id=9)

---

### Dead Letter Table (Failed Quality Checks)
```
| id | name   | age  | quarantine_timestamp | quarantine_reason | moved_to_dead_letter |
|----|--------|------|---------------------|-------------------|---------------------|
| 3  | NULL   | 25   | 2026-03-06 14:30:22 | null_name         | 2026-03-07 15:00:00 |
| 9  | George | NULL | 2026-03-06 15:00:00 | null_age          | 2026-03-07 15:00:00 |
```
**Count:** 2 records (never fixed within 24 hours)

---

### Silver Table (SCD Type 2 - From Bronze CDC)
```
| id | name         | age  | __start_at          | __end_at            | __is_current |
|----|--------------|------|---------------------|---------------------|--------------|
| 1  | Alice        | 30   | 2026-03-06 14:30:22 | 2026-03-06 15:00:00 | false        |
| 1  | Alice Smith  | 31   | 2026-03-06 15:00:00 | NULL                | true         |
| 2  | Bob Johnson  | 40   | 2026-03-06 15:00:00 | NULL                | true         |
| 4  | Charlie      | 35   | 2026-03-06 14:30:22 | NULL                | true         |
```
**Count:** 4 records (includes historical versions)
- Current records: 3 (id=1 latest, id=2, id=4)
- Historical records: 1 (id=1 old version)

---

## Summary: Record Journey

### Record id=1 (Alice)
```
Landing Zone (Full) → Bronze (Full Load) → Validated Clean → Bronze (Quality Check)
                                                             ↓
Landing Zone (Partial - Update) → Bronze (Partial Merge) → Validated Clean → Bronze (Quality Check)
                                                                              ↓
                                                                         Silver (SCD2 - 2 versions)
```

### Record id=2 (Bob) - **The Fix Journey**
```
Landing Zone (Full) → Bronze (Full Load) → Validated BAD → Quarantine (v1 - null age)
                                           ↓
Landing Zone (Partial - Fix) → Bronze (Partial Merge) → Validated CLEAN but ID quarantined
                                                        → Quarantine (v2 - STACKED with age=40)
                                                        ↓
                               Quarantine Processor detects v2 is valid
                                                        ↓
                               Both versions marked FIXED → Latest version (v2) → Bronze
                                                                                    ↓
                                                                               Silver (SCD2)
```

### Record id=3 (null name) - **The Expiry Journey**
```
Landing Zone (Full) → Bronze (Full Load) → Validated BAD → Quarantine (null_name, ACTIVE)
                                           ↓
                               Wait 24 hours... no fix arrives
                                           ↓
                               Quarantine Processor detects expiry
                                           ↓
                               Dead Letter Table (EXPIRED)
                               Quarantine status → EXPIRED
```

### Record id=4 (Charlie) - **The Smooth Journey**
```
Landing Zone (Full) → Bronze (Full Load) → Validated Clean → Bronze (Quality Check) → Silver (SCD2)
```

### Record id=9 (George) - **The New Bad Record Journey**
```
Landing Zone (Partial) → Bronze (Partial Merge) → Validated BAD → Quarantine (null_age, ACTIVE)
                                                   ↓
                               Wait 24 hours... no fix arrives
                                                   ↓
                               Quarantine Processor detects expiry
                                                   ↓
                               Dead Letter Table (EXPIRED)
                               Quarantine status → EXPIRED
```

---

## Key Insights

1. **Bronze contains bad data temporarily** (between Full/Partial Load and Quality Check)
2. **Quality Check cleans Bronze** by removing bad records and routing them to Quarantine
3. **ID Stacking works**: Record id=2 appears twice in Quarantine (bad version + good version)
4. **Fix detection** identifies when ALL columns are non-null in Quarantine
5. **Latest version wins**: When multiple versions exist, the most recent is released to Bronze
6. **Expiry is automatic**: After 24 hours without fix, records move to Dead Letter
7. **Historical tracking**: Quarantine keeps full history with status transitions
8. **Silver gets clean data only**: SCD Type 2 tracks changes from Bronze (which only has validated data)
