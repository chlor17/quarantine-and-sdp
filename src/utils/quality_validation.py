"""Quality validation and routing for quarantine-sdp pipeline."""

from typing import Dict, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.utils.constants import (
    QUARANTINE_STATUS_QUARANTINE,
    QUARANTINE_STATUS_BLOCKED,
)
from src.utils.quarantine_ops import get_active_quarantined_ids


def validate_record(
    df: DataFrame, required_columns: List[str], source_path: str = ""
) -> DataFrame:
    """
    Add validation columns to identify quality issues.

    Args:
        df: Input DataFrame to validate
        required_columns: List of column names that cannot be null
        source_path: Path to source file for audit trail

    Returns:
        DataFrame with validation columns:
        - has_any_null: Boolean if any required column is null
        - validation_errors: Array of error codes for null columns
        - source_file: Source file path
    """
    # Input validation
    if not isinstance(df, DataFrame):
        raise TypeError(f"Expected DataFrame, got {type(df)}")

    missing_cols = set(required_columns) - set(df.columns)
    if missing_cols:
        raise ValueError(f"Missing required columns: {missing_cols}")

    # Create null checks for each column
    null_checks = []

    for col_name in required_columns:
        null_check_col = f"has_null_{col_name}"
        null_checks.append(F.col(null_check_col))
        df = df.withColumn(null_check_col, F.col(col_name).isNull())

    # Combine all null checks
    has_any_null = null_checks[0]
    for check in null_checks[1:]:
        has_any_null = has_any_null | check

    # Build validation errors array using filter to remove nulls
    validation_errors = F.expr(
        "filter(array("
        + ", ".join([f"CASE WHEN {col} IS NULL THEN 'null_{col}' END" for col in required_columns])
        + "), x -> x IS NOT NULL)"
    )

    return (
        df.withColumn("has_any_null", has_any_null)
        .withColumn("validation_errors", validation_errors)
        .withColumn("source_file", F.lit(source_path))
    )


def quality_check_and_route(
    spark, incoming_df: DataFrame, config: dict, source_path: str, mode: str = "append"
) -> Dict[str, int]:
    """
    Route data to Bronze or Quarantine based on quality.

    Process:
    1. Get currently quarantined IDs
    2. Validate incoming DataFrame
    3. Route based on quality AND quarantine status:
       - Clean records (no nulls, not quarantined) → Bronze
       - Bad records (has nulls OR already quarantined) → Quarantine

    Returns:
        dict with clean_count, quarantine_count, total_processed
    """
    catalog = config["Catalog"]
    schema = config["Schema"]
    bronze_table = config["Bronze_table"]
    quarantine_table = config["Quarantine_table"]
    required_columns = config["Required_Columns"]
    quarantine_hours = config.get("Quarantine_Hours", 24)

    # Get currently quarantined IDs
    quarantined_ids = get_active_quarantined_ids(
        spark, catalog, schema, quarantine_table
    )

    # Apply quality checks
    validated_df = validate_record(incoming_df, required_columns, source_path)

    # Split into clean and quarantine
    clean_df = validated_df.filter(
        ~F.col("has_any_null") & ~F.col("id").isin(quarantined_ids)
    ).select(*required_columns)

    # Add flag to identify ID stacking cases and set appropriate status
    # Priority: Data quality issues (has_any_null) take precedence over ID stacking
    quarantine_df = (
        validated_df.filter(F.col("has_any_null") | F.col("id").isin(quarantined_ids))
        .withColumn("is_id_stacked", F.col("id").isin(quarantined_ids))
        .withColumn(
            "quarantine_reason",
            F.when(
                F.col("has_any_null"),
                F.concat_ws(", ", "validation_errors"),
            ).otherwise(F.lit("ID already quarantined (ID Stacking)")),
        )
        .withColumn(
            "quarantine_status",
            F.when(
                F.col("has_any_null"),
                F.lit(QUARANTINE_STATUS_QUARANTINE),  # Data quality issue
            ).otherwise(F.lit(QUARANTINE_STATUS_BLOCKED)),  # ID stacking
        )
        .select(
            *required_columns,
            F.current_timestamp().alias("quarantine_timestamp"),
            F.expr(
                f"current_timestamp() + INTERVAL {quarantine_hours} HOURS"
            ).alias("quarantine_expiry"),
            "quarantine_status",
            "validation_errors",
            "quarantine_reason",
            F.lit(0).alias("fix_attempts"),
            F.col("source_file"),
            F.current_timestamp().alias("ingestion_timestamp"),
        )
    )

    clean_count = clean_df.count()
    quarantine_count = quarantine_df.count()

    # Write clean records to Bronze
    if clean_count > 0:
        clean_df.write.format("delta").mode(mode).saveAsTable(
            f"{catalog}.{schema}.{bronze_table}"
        )

    # Write failed records to Quarantine
    if quarantine_count > 0:
        quarantine_df.write.format("delta").mode("append").saveAsTable(
            f"{catalog}.{schema}.{quarantine_table}"
        )

    return {
        "clean_count": clean_count,
        "quarantine_count": quarantine_count,
        "total_processed": clean_count + quarantine_count,
    }
