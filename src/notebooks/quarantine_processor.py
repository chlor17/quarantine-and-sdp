# Databricks notebook source
# DBTITLE 1,Quarantine Maintenance
"""
Background maintenance for quarantine table (runs hourly)
- Process expired records
- Detect and release fixed records
- Health monitoring and alerts
"""

# COMMAND ----------

import sys
import os
from pyspark.sql import functions as F

# Load config first to get workspace_root
from src.utils.config import load_config
config = load_config()

# Add workspace files to Python path for imports (if configured)
workspace_root = config.get('Workspace_Root')
if workspace_root and workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)
from src.utils.quarantine_ops import (
    detect_and_release_fixed_records,
    move_expired_to_dead_letter,
    promote_blocked_records,
    get_quarantine_health_stats
)

# COMMAND ----------

# Load config
config = load_config()

catalog = config['Catalog']
schema = config['Schema']
quarantine_table = config['Quarantine_table']
dead_letter_table = config['Dead_letter_table']
bronze_table = config['Bronze_table']
required_columns = config['Required_Columns']

quarantine_full_name = f"{catalog}.{schema}.{quarantine_table}"

# COMMAND ----------

# Get quarantine health stats
stats = get_quarantine_health_stats(spark, catalog, schema, quarantine_table)

print("="*60)
print("QUARANTINE HEALTH CHECK")
print("="*60)
print(f"QUARANTINE (quality issues): {stats.get('quarantine', 0)} records")
print(f"BLOCKED (ID stacking): {stats.get('blocked', 0)} records")
print(f"Active (legacy): {stats.get('active', 0)} records")
print(f"Fixed: {stats['fixed']} records")
print(f"Released: {stats['released']} records")
print(f"Expired: {stats['expired']} records")
print(f"Total: {stats['total']} records")

# Alert if active quarantine is high
total_active = stats.get('quarantine', 0) + stats.get('blocked', 0) + stats.get('active', 0)
if total_active > 1000:
    print(f"\n⚠️  HIGH QUARANTINE COUNT: {total_active} active records")

# COMMAND ----------

# Check records expiring soon (next 6 hours)
expiring_soon = spark.sql(f"""
    SELECT
        id,
        quarantine_status,
        quarantine_reason,
        ROUND((unix_timestamp(quarantine_expiry) - unix_timestamp(current_timestamp())) / 3600, 2) as hours_remaining
    FROM {quarantine_full_name}
    WHERE quarantine_status IN ('QUARANTINE', 'BLOCKED', 'ACTIVE')
      AND quarantine_expiry < current_timestamp() + INTERVAL 6 HOURS
      AND quarantine_expiry > current_timestamp()
    ORDER BY quarantine_expiry
    LIMIT 10
""").collect()

if expiring_soon:
    print(f"\n⚠️  {len(expiring_soon)} records expiring in next 6 hours:")
    for row in expiring_soon[:5]:  # Show first 5
        print(f"   ID={row.id}, Reason={row.quarantine_reason}, Hours={row.hours_remaining}")

# COMMAND ----------

# Process expired records
expired_count = move_expired_to_dead_letter(
    spark,
    catalog,
    schema,
    quarantine_table,
    dead_letter_table
)

if expired_count > 0:
    print(f"⚠️  Moved {expired_count} expired records to Dead Letter")
else:
    print("No expired records to process")

# COMMAND ----------

# Detect and release fixed records
released_count = detect_and_release_fixed_records(
    spark,
    catalog,
    schema,
    quarantine_table,
    bronze_table,
    required_columns
)

if released_count > 0:
    print(f"✅ Released {released_count} fixed QUARANTINE records to Bronze")
else:
    print("No fixed QUARANTINE records to release")

# COMMAND ----------

# Promote BLOCKED records when blocking condition clears
promoted_count = promote_blocked_records(
    spark,
    catalog,
    schema,
    quarantine_table,
    bronze_table,
    required_columns
)

if promoted_count > 0:
    print(f"✅ Promoted {promoted_count} BLOCKED records to Bronze")
else:
    print("No BLOCKED records to promote")

# COMMAND ----------

# Top quarantine reasons
top_reasons = spark.sql(f"""
    SELECT
        quarantine_status,
        quarantine_reason,
        COUNT(*) as count,
        COUNT(DISTINCT id) as unique_ids
    FROM {quarantine_full_name}
    WHERE quarantine_status IN ('QUARANTINE', 'BLOCKED', 'ACTIVE')
    GROUP BY quarantine_status, quarantine_reason
    ORDER BY count DESC
    LIMIT 10
""")

if top_reasons.count() > 0:
    print("\n" + "="*60)
    print("TOP QUARANTINE REASONS")
    print("="*60)
    top_reasons.show(truncate=False)
else:
    print("\nNo active quarantine records")

# COMMAND ----------

# Oldest active quarantine records
oldest_records = spark.sql(f"""
    SELECT
        id,
        quarantine_status,
        quarantine_reason,
        ROUND((unix_timestamp(current_timestamp()) - unix_timestamp(quarantine_timestamp)) / 3600, 2) as hours_quarantined
    FROM {quarantine_full_name}
    WHERE quarantine_status IN ('QUARANTINE', 'BLOCKED', 'ACTIVE')
    ORDER BY quarantine_timestamp
    LIMIT 10
""")

if oldest_records.count() > 0:
    print("\n" + "="*60)
    print("OLDEST ACTIVE QUARANTINE RECORDS")
    print("="*60)
    oldest_records.show(truncate=False)

# COMMAND ----------

# Final summary
print("\n" + "="*60)
print("MAINTENANCE SUMMARY")
print("="*60)
print(f"QUARANTINE (quality): {stats.get('quarantine', 0)} records")
print(f"BLOCKED (ID stacking): {stats.get('blocked', 0)} records")
print(f"Expired & Moved: {expired_count} records")
print(f"Fixed QUARANTINE Released: {released_count} records")
print(f"BLOCKED Promoted: {promoted_count} records")
print("="*60)
