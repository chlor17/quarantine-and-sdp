# Databricks notebook source
# DBTITLE 1,Partial Load with Quality Routing
"""
Partial Load: Landing Zone → Quality Check → Bronze + Quarantine (parallel)
- Validates data upfront
- Clean records → Bronze (MERGE/UPSERT)
- Invalid records → Quarantine (APPEND)
- ID Stacking: Quarantined IDs stay quarantined until fixed
"""

# COMMAND ----------

import sys

# Add workspace files to Python path for imports
workspace_root = "/Workspace/Users/chad.lortie@databricks.com/2026-03-06 Pharos/files"
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

# Import utilities
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from src.utils.config import load_config
from src.utils.file_discovery import get_latest_parquet_path
from src.utils.schema_definitions import create_bronze_table, create_quarantine_table
from src.utils.constants import (
    QUARANTINE_STATUS_QUARANTINE,
    QUARANTINE_STATUS_BLOCKED
)
from src.utils.quarantine_ops import get_active_quarantined_ids

# COMMAND ----------

# Load config
config = load_config()

catalog = config['Catalog']
schema = config['Schema']
volume_partial = config['Vol_Partial']
bronze_table = config['Bronze_table']
quarantine_table = config['Quarantine_table']
required_columns = config['Required_Columns']
quarantine_hours = config.get('Quarantine_Hours', 24)

# COMMAND ----------

# Create Bronze table (let quarantine table be created automatically)
create_bronze_table(spark, catalog, schema, bronze_table)
# DON'T pre-create quarantine - let DataFrame write create it

# COMMAND ----------

# Get currently quarantined IDs (ID Stacking pattern)
quarantined_ids = get_active_quarantined_ids(spark, catalog, schema, quarantine_table)

print(f"🔒 Currently quarantined IDs: {len(quarantined_ids)}")

# COMMAND ----------

# Get latest parquet folder
latest_path = get_latest_parquet_path(catalog, schema, volume_partial)
print(f"📂 Reading from: {latest_path}")

# COMMAND ----------

# Read and validate data
incoming_df = spark.read.parquet(latest_path)
total_count = incoming_df.count()

# Add validation flags
validated_df = incoming_df
for col in required_columns:
    validated_df = validated_df.withColumn(
        f"{col}_is_null",
        F.col(col).isNull()
    )

# Mark records with any null
validated_df = validated_df.withColumn("has_any_null", F.expr(" OR ".join([f"{col}_is_null" for col in required_columns])))

# Mark records with quarantined IDs (ID Stacking)
validated_df = validated_df.withColumn("id_is_quarantined", F.col('id').isin(quarantined_ids))

# Split: clean records go to Bronze, invalid or quarantined IDs go to Quarantine
clean_df = validated_df.filter(~F.col('has_any_null') & ~F.col('id_is_quarantined')).select(*required_columns)
quarantine_df = validated_df.filter(F.col('has_any_null') | F.col('id_is_quarantined'))

clean_count = clean_df.count()
quarantine_count = quarantine_df.count()

print(f"📊 Validation Results:")
print(f"   Total: {total_count}")
print(f"   Clean: {clean_count}")
print(f"   Quarantine: {quarantine_count}")

# COMMAND ----------

# MERGE clean data into Bronze (UPSERT on id)
if clean_count > 0:
    bronze_full_name = f"{catalog}.{schema}.{bronze_table}"
    bronze_before = spark.table(bronze_full_name).count()

    delta_table = DeltaTable.forName(spark, bronze_full_name)

    (delta_table.alias("bronze")
        .merge(
            clean_df.alias("partial"),
            "bronze.id = partial.id"
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

    bronze_after = spark.table(bronze_full_name).count()
    records_changed = bronze_after - bronze_before

    print(f"✅ Merged {clean_count} clean records to Bronze")
    print(f"   Before: {bronze_before}, After: {bronze_after} ({'+' if records_changed >= 0 else ''}{records_changed})")
else:
    print(f"⚠️  No clean records to merge")

# COMMAND ----------

# Write invalid data to Quarantine (APPEND)
if quarantine_count > 0:
    # Build error array and quarantine metadata with conditional logic
    # Keep id_is_quarantined column until after we use it
    quarantine_with_meta = quarantine_df.withColumn(
        "errors_array",
        F.expr(
            "filter(array("
            + ", ".join([f"CASE WHEN {c} IS NULL THEN 'null_{c}' END" for c in required_columns])
            + "), x -> x IS NOT NULL)"
        ),
    ).withColumn(
        "validation_errors",
        F.when(F.col("has_any_null"), F.col("errors_array"))
         .otherwise(F.array(F.lit("id_already_quarantined")))
    ).withColumn(
        "quarantine_reason",
        F.when(F.col("has_any_null"), F.concat_ws(", ", F.col("errors_array")))
         .otherwise(F.lit("ID already quarantined (ID Stacking)"))
    ).withColumn(
        "quarantine_timestamp", F.current_timestamp()
    ).withColumn(
        "quarantine_expiry",
        F.expr(f"current_timestamp() + INTERVAL {quarantine_hours} HOURS")
    ).withColumn(
        "quarantine_status",
        F.when(
            F.col("has_any_null"),
            F.lit(QUARANTINE_STATUS_QUARANTINE)  # Data quality issue
        ).otherwise(F.lit(QUARANTINE_STATUS_BLOCKED))  # ID stacking
    ).withColumn(
        "fix_attempts", F.lit(0)
    ).withColumn(
        "source_file", F.lit(latest_path)
    ).withColumn(
        "ingestion_timestamp", F.current_timestamp()
    ).select(
        *required_columns,
        "quarantine_timestamp",
        "quarantine_expiry",
        "quarantine_status",
        "validation_errors",
        "quarantine_reason",
        "fix_attempts",
        "source_file",
        "ingestion_timestamp"
    )

    quarantine_with_meta.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable(f"{catalog}.{schema}.{quarantine_table}")

    print(f"⚠️  Wrote {quarantine_count} records to Quarantine")
    print(f"   Expiry: {quarantine_hours} hours from now")
else:
    print(f"✅ No records quarantined")

# COMMAND ----------

print(f"\n{'='*60}")
print(f"PARTIAL LOAD COMPLETE")
print(f"{'='*60}")
print(f"Bronze: {clean_count} records merged")
print(f"Quarantine: {quarantine_count} records")
print(f"\n💡 To fix quarantine records: See QUARANTINE_FIX_GUIDE.md")
