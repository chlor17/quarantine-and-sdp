# Databricks notebook source
# DBTITLE 1,Quality Check and Routing
"""
Quality Check: Validates Landing Zone data and routes to Bronze or Quarantine
- Also handles fixed records (quarantine → bronze)
- Also handles expired records (quarantine → dead letter)
"""

# COMMAND ----------

import os
import sys

# Add workspace files to Python path for imports
workspace_root = "/Workspace/Users/chad.lortie@databricks.com/2026-03-06 Pharos/files"

if workspace_root not in sys.path:
    sys.path.insert(0, workspace_root)

# Import utilities
from pyspark.sql import functions as F

from src.utils.config import load_config
from src.utils.file_discovery import get_latest_from_multiple_volumes
from src.utils.quality_validation import quality_check_and_route
from src.utils.quarantine_ops import (
    detect_and_release_fixed_records,
    get_quarantine_health_stats,
    move_expired_to_dead_letter,
)
from src.utils.schema_definitions import (
    create_dead_letter_table,
    create_quarantine_table,
)

# COMMAND ----------

# Load config
config = load_config()

# COMMAND ----------

# Create tables if needed
create_quarantine_table(
    spark, config["Catalog"], config["Schema"], config["Quarantine_table"]
)
create_dead_letter_table(
    spark, config["Catalog"], config["Schema"], config["Dead_letter_table"]
)

# COMMAND ----------

# Get latest data from both volumes
latest_data = get_latest_from_multiple_volumes(
    config["Catalog"], config["Schema"], [config["Vol_Full"], config["Vol_Partial"]]
)

print(f"Found {len(latest_data)} landing zone batches to process")

# COMMAND ----------

# Process each batch
total_clean = 0
total_quarantine = 0

for volume_name, folder_name, latest_path in latest_data:
    print(f"Processing {volume_name}/{folder_name}")

    df = spark.read.parquet(latest_path)

    result = quality_check_and_route(
        spark, incoming_df=df, config=config, source_path=latest_path, mode="append"
    )

    total_clean += result["clean_count"]
    total_quarantine += result["quarantine_count"]

    print(f"  Clean: {result['clean_count']}, Quarantine: {result['quarantine_count']}")

# COMMAND ----------

# Release fixed records
released_count = detect_and_release_fixed_records(
    spark,
    config["Catalog"],
    config["Schema"],
    config["Quarantine_table"],
    config["Bronze_table"],
    config["Required_Columns"],
)

if released_count > 0:
    print(f"✅ Released {released_count} fixed records from quarantine")

# COMMAND ----------

# Move expired records to dead letter
expired_count = move_expired_to_dead_letter(
    spark,
    config["Catalog"],
    config["Schema"],
    config["Quarantine_table"],
    config["Dead_letter_table"],
)

if expired_count > 0:
    print(f"⚠️  Moved {expired_count} expired records to dead letter")

# COMMAND ----------

# Show stats
stats = get_quarantine_health_stats(
    spark, config["Catalog"], config["Schema"], config["Quarantine_table"]
)

print("\n" + "=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"Clean Records: {total_clean}")
print(f"Quarantine Records: {total_quarantine}")
print(f"Fixed & Released: {released_count}")
print(f"Expired & Dead Lettered: {expired_count}")
print(f"\nActive Quarantine: {stats['active']} records")
print(f"Total Quarantine (all statuses): {stats['total']} records")
print("=" * 60)
