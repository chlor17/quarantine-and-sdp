# Databricks notebook source
# DBTITLE 1,Silver SCD Type 2 Pipeline
"""
Silver Table with SCD Type 2 tracking using Spark Declarative Pipelines
Reads Bronze CDF and tracks historical changes using AUTO CDC
"""

# COMMAND ----------

import os

from pyspark import pipelines as dp
from pyspark.sql.functions import col, expr

# Get config from DAB environment variables
catalog = os.getenv("source_catalog", "your_catalog")
schema = os.getenv("source_schema", "your_schema")
bronze_table = os.getenv("bronze_table", "bronze_table")

# COMMAND ----------

# Create streaming table for SCD Type 2
# Uses bronze table directly (bronze has CDF enabled)
bronze_full_name = f"{catalog}.{schema}.{bronze_table}"


@dp.view(name="bronze_table")
def bronze_table():
    """Read change data feed from bronze table"""
    return (
        spark.readStream.format("delta")
        .option("readChangeFeed", "true")
        .table(bronze_full_name)
    )


dp.create_streaming_table(
    name="silver_table",
    comment="Silver table with SCD Type 2 historical tracking via AUTO CDC",
    table_properties={"pipelines.autoOptimize.zOrderCols": "id", "quality": "silver"},
)

# Apply AUTO CDC for SCD Type 2 - reads directly from bronze table
# Bronze table has CDF enabled, so apply_changes will read from CDF automatically
dp.create_auto_cdc_flow(
    target="silver_table",
    source="bronze_table",
    keys=["id"],
    sequence_by="_commit_timestamp",
    stored_as_scd_type=2,
    except_column_list=["_change_type", "_commit_timestamp", "_commit_version"],
    apply_as_deletes=expr(
        "_change_type = 'delete' or _change_type = 'update_preimage'"
    ),
    track_history_except_column_list=[
        "_change_type",
        "_commit_version",
        "_commit_timestamp",
    ],
)
