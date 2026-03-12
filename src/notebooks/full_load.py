# Databricks notebook source
# DBTITLE 1,Full Load with Quality Routing
"""
Full Load: Landing Zone → Quality Check → Bronze + Quarantine (parallel)
- Validates data upfront
- Clean records → Bronze (OVERWRITE)
- Invalid records → Quarantine (APPEND)
"""

# COMMAND ----------

import sys

# Add workspace files to Python path for imports
workspace_root = "/Workspace/Users/chad.lortie@databricks.com/2026-03-06 Pharos/files"
if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

from datetime import datetime

# Import utilities
from pyspark.sql import functions as F

from src.utils.config import load_config
from src.utils.file_discovery import get_latest_parquet_path
from src.utils.schema_definitions import create_bronze_table, create_quarantine_table
from src.utils.constants import QUARANTINE_STATUS_QUARANTINE

# COMMAND ----------

# Load config
config = load_config()

catalog = config["Catalog"]
schema = config["Schema"]
volume_full = config["Vol_Full"]
bronze_table = config["Bronze_table"]
quarantine_table = config["Quarantine_table"]
required_columns = config["Required_Columns"]
quarantine_hours = config.get("Quarantine_Hours", 24)

# COMMAND ----------

# Create Bronze table (let quarantine table be created automatically)
create_bronze_table(spark, catalog, schema, bronze_table)
# DON'T pre-create quarantine - let DataFrame write create it

# COMMAND ----------

# Get latest parquet folder
latest_path = get_latest_parquet_path(catalog, schema, volume_full)
print(f"📂 Reading from: {latest_path}")

# COMMAND ----------

# Read and validate data
incoming_df = spark.read.parquet(latest_path)
total_count = incoming_df.count()

# Add validation flags
validated_df = incoming_df
for col in required_columns:
    validated_df = validated_df.withColumn(f"{col}_is_null", F.col(col).isNull())

# Mark records with any null
null_checks = [F.col(f"{col}_is_null") for col in required_columns]
validated_df = validated_df.withColumn(
    "has_any_null", F.expr(" OR ".join([f"{col}_is_null" for col in required_columns]))
)

# Split into clean and quarantine
clean_df = validated_df.filter(~F.col("has_any_null")).select(*required_columns)
quarantine_df = validated_df.filter(F.col("has_any_null"))

clean_count = clean_df.count()
quarantine_count = quarantine_df.count()

print(f"📊 Validation Results:")
print(f"   Total: {total_count}")
print(f"   Clean: {clean_count}")
print(f"   Quarantine: {quarantine_count}")

# COMMAND ----------

# Write clean data to Bronze (OVERWRITE for full load)
clean_df.write.format("delta").mode("overwrite").option(
    "delta.enableChangeDataFeed", "true"
).option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.{bronze_table}")

print(f"✅ Wrote {clean_count} clean records to Bronze")

# COMMAND ----------

# Write invalid data to Quarantine (APPEND)
if quarantine_count > 0:
    # Add quarantine metadata (matching schema_definitions.py)
    quarantine_with_meta = (
        quarantine_df
        .withColumn(
            "errors_array",
            F.expr(
                "filter(array("
                + ", ".join([f"CASE WHEN {c} IS NULL THEN 'null_{c}' END" for c in required_columns])
                + "), x -> x IS NOT NULL)"
            ),
        )
        .withColumn("validation_errors", F.col("errors_array"))
        .withColumn("quarantine_reason", F.concat_ws(", ", F.col("errors_array")))
        .withColumn("quarantine_timestamp", F.current_timestamp())
        .withColumn(
            "quarantine_expiry",
            F.expr(f"current_timestamp() + INTERVAL {quarantine_hours} HOURS"),
        )
        .withColumn("quarantine_status", F.lit(QUARANTINE_STATUS_QUARANTINE))
        .withColumn("fix_attempts", F.lit(0))
        .withColumn("source_file", F.lit(latest_path))
        .withColumn("ingestion_timestamp", F.current_timestamp())
        .select(
            *required_columns,
            "quarantine_timestamp",
            "quarantine_expiry",
            "quarantine_status",
            "validation_errors",
            "quarantine_reason",
            "fix_attempts",
            "source_file",
            "ingestion_timestamp",
        )
    )

    # Debug: Print schema before write
    print(f"🐛 DEBUG: Quarantine DataFrame schema:")
    quarantine_with_meta.printSchema()
    print(f"🐛 DEBUG: Column names: {quarantine_with_meta.columns}")

    quarantine_with_meta.write.format("delta").mode("append").option(
        "mergeSchema", "true"
    ).saveAsTable(f"{catalog}.{schema}.{quarantine_table}")

    print(f"⚠️  Wrote {quarantine_count} records to Quarantine")
    print(f"   Expiry: {quarantine_hours} hours from now")
else:
    print(f"✅ No records quarantined")

# COMMAND ----------

print(f"\n{'='*60}")
print(f"FULL LOAD COMPLETE")
print(f"{'='*60}")
print(f"Bronze: {clean_count} records")
print(f"Quarantine: {quarantine_count} records")
print(f"\n💡 To fix quarantine records: Update source data and re-run")
