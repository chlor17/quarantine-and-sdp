# Quarantine Fix Guide

This guide explains how to manually fix records that have been quarantined due to data quality issues.

## Understanding Quarantine

Records are quarantined when they fail validation checks. Common reasons:
- **Null values** in required columns (id, name, age)
- **ID Stacking**: Records with IDs that are already quarantined remain quarantined until fixed

Quarantined records have an expiry time (default: 24 hours). After expiry, they move to the Dead Letter table.

---

## Viewing Quarantine Records

### Check Current Quarantine Status

```sql
-- View all active quarantine records
SELECT id, name, age, reason, quarantine_timestamp, quarantine_expiry
FROM chlor.Pharos.quarantine_table
WHERE is_released = FALSE
ORDER BY quarantine_timestamp DESC;
```

### Check Specific Records

```sql
-- Check records for a specific ID
SELECT *
FROM chlor.Pharos.quarantine_table
WHERE id = 3
  AND is_released = FALSE;
```

### Group by Reason

```sql
-- Count quarantine records by reason
SELECT reason, COUNT(*) as count
FROM chlor.Pharos.quarantine_table
WHERE is_released = FALSE
GROUP BY reason
ORDER BY count DESC;
```

---

## Method 1: Fix Source Data and Re-run Pipeline

**Best for**: Large batches of records with consistent issues

1. **Identify the problematic records**:
   ```sql
   SELECT id, name, age, reason, source_file
   FROM chlor.Pharos.quarantine_table
   WHERE is_released = FALSE;
   ```

2. **Fix the source data** in your upstream system or landing zone volume

3. **Re-run the pipeline**:
   - For full load: Run `full_load_pipeline`
   - For partial load: Run `partial_load_pipeline` with corrected records

4. **Verify the fix**:
   ```sql
   -- Check if records moved to Bronze
   SELECT id, name, age
   FROM chlor.Pharos.bronze_table
   WHERE id IN (3, 4, 8);  -- Your fixed IDs
   ```

---

## Method 2: Direct SQL Update (Quick Fix)

**Best for**: Small number of records with simple fixes

### Fix NULL name

```sql
-- Update name directly in quarantine table
UPDATE chlor.Pharos.quarantine_table
SET name = 'Unknown'
WHERE id = 3
  AND is_released = FALSE;
```

### Fix NULL age

```sql
-- Update age directly
UPDATE chlor.Pharos.quarantine_table
SET age = 0  -- or appropriate default
WHERE id = 4
  AND is_released = FALSE;
```

### Then release fixed records:

```sql
-- Mark as released so they can be processed
UPDATE chlor.Pharos.quarantine_table
SET is_released = TRUE
WHERE id IN (3, 4)
  AND is_released = FALSE;
```

### Run quarantine processor to move to Bronze:

Run the `quarantine_processor` notebook or job to automatically detect and release fixed records to Bronze.

---

## Method 3: Bulk Fix with MERGE

**Best for**: Batch correction with a corrected dataset

```sql
-- Create a temp view with corrected data
CREATE OR REPLACE TEMP VIEW corrected_records AS
SELECT 3 AS id, 'Fixed Name' AS name, 42 AS age
UNION ALL
SELECT 4 AS id, 'Diana Prince' AS name, 35 AS age
UNION ALL
SELECT 8 AS id, 'Corrected Name' AS name, 28 AS age;

-- Merge corrections into quarantine table
MERGE INTO chlor.Pharos.quarantine_table AS quarantine
USING corrected_records AS corrections
ON quarantine.id = corrections.id
  AND quarantine.is_released = FALSE
WHEN MATCHED THEN
  UPDATE SET
    quarantine.name = corrections.name,
    quarantine.age = corrections.age;

-- Mark as released
UPDATE chlor.Pharos.quarantine_table
SET is_released = TRUE
WHERE id IN (SELECT id FROM corrected_records)
  AND is_released = FALSE;
```

---

## Method 4: Python/PySpark Script

**Best for**: Complex transformations or automated fixes

```python
from pyspark.sql import functions as F

# Read quarantine table
quarantine_df = spark.table("chlor.Pharos.quarantine_table").filter("is_released = FALSE")

# Apply fixes (example: fill nulls with defaults)
fixed_df = quarantine_df \
    .withColumn("name", F.coalesce(F.col("name"), F.lit("Unknown"))) \
    .withColumn("age", F.coalesce(F.col("age"), F.lit(0)))

# Write fixed records back (this creates new rows, so you need to mark old ones as released)
# Better approach: use Delta MERGE operation

from delta.tables import DeltaTable

quarantine_table = DeltaTable.forName(spark, "chlor.Pharos.quarantine_table")

(quarantine_table.alias("q")
    .merge(
        fixed_df.alias("f"),
        "q.id = f.id AND q.is_released = FALSE"
    )
    .whenMatchedUpdate(set={
        "name": "f.name",
        "age": "f.age"
    })
    .execute()
)

# Mark as released
spark.sql("""
    UPDATE chlor.Pharos.quarantine_table
    SET is_released = TRUE
    WHERE id IN (SELECT DISTINCT id FROM corrected_ids)
      AND is_released = FALSE
""")
```

---

## Verification Steps

After fixing records, always verify:

### 1. Check Quarantine Status

```sql
-- Should return 0 for fixed IDs
SELECT COUNT(*)
FROM chlor.Pharos.quarantine_table
WHERE id IN (3, 4, 8)
  AND is_released = FALSE;
```

### 2. Check Bronze Table

```sql
-- Fixed records should appear here
SELECT id, name, age
FROM chlor.Pharos.bronze_table
WHERE id IN (3, 4, 8);
```

### 3. Check Silver Table (if DLT pipeline has run)

```sql
-- Should propagate to Silver via CDC
SELECT id, name, age, is_current
FROM chlor.Pharos.silver_table
WHERE id IN (3, 4, 8)
  AND is_current = TRUE;
```

---

## Preventing Future Quarantine

### 1. Upstream Data Quality Checks

Implement validation in your data source before landing in Databricks:
- Ensure required fields are always populated
- Use default values for optional fields
- Validate data types and ranges

### 2. Adjust Validation Rules

If quarantine rules are too strict, update `config.yaml`:

```yaml
Required_Columns: ["id"]  # Make only id required
Quarantine_Hours: 48  # Give more time to fix issues
```

### 3. Monitor Quarantine Metrics

```sql
-- Daily quarantine trend
SELECT
  DATE(quarantine_timestamp) as date,
  COUNT(*) as quarantined_count,
  SUM(CASE WHEN is_released THEN 1 ELSE 0 END) as released_count
FROM chlor.Pharos.quarantine_table
GROUP BY DATE(quarantine_timestamp)
ORDER BY date DESC;
```

---

## Automated Fix Workflow

The `quarantine_processor` notebook runs hourly to:
1. **Detect fixed records**: Checks if previously quarantined IDs now have valid data
2. **Release to Bronze**: Moves fixed records to the Bronze table
3. **Expire old records**: Moves records past their expiry to Dead Letter table

Schedule: Runs every hour (adjust in `resources/jobs.yml`)

---

## Troubleshooting

### Records Won't Release

**Issue**: Updated records still quarantined

**Solution**: Check ID Stacking - once an ID is quarantined, ALL future records with that ID are quarantined until the original issue is fixed.

```sql
-- Check all quarantine history for an ID
SELECT *
FROM chlor.Pharos.quarantine_table
WHERE id = 3
ORDER BY quarantine_timestamp;
```

### Records Expired Too Quickly

**Issue**: Records moved to Dead Letter before you could fix them

**Solution**:
1. Increase `Quarantine_Hours` in `config.yaml`
2. Recover from Dead Letter table:

```sql
-- View dead letter records
SELECT * FROM chlor.Pharos.dead_letter_table
WHERE id = 3;

-- Copy back to quarantine with new expiry
INSERT INTO chlor.Pharos.quarantine_table
SELECT
  id, name, age,
  current_timestamp() as quarantine_timestamp,
  current_timestamp() + INTERVAL 24 HOURS as quarantine_expiry,
  source_file, reason,
  FALSE as is_released
FROM chlor.Pharos.dead_letter_table
WHERE id = 3;
```

---

## Support

For questions or issues:
- Check logs in Databricks Job runs
- Review quarantine stats: `SELECT * FROM chlor.Pharos.quarantine_table WHERE is_released = FALSE`
- Contact your data engineering team
