"""
Table schema definitions for demo_marketing pipeline.

Centralizes all CREATE TABLE DDL statements to eliminate duplication.
"""

from typing import List
import logging

from src.utils.constants import DELTA_ENABLE_CDF



def create_bronze_table(
    spark,
    catalog: str,
    schema: str,
    table_name: str,
    columns: List[str] = None
) -> None:
    """
    Create Bronze table with Change Data Feed enabled.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table_name: Table name
        columns: List of column definitions (default: id INT, name STRING, age INT)

    Example:
        >>> create_bronze_table(spark, "chlor", "dp_ingestion_one_flow", "bronze_table")
    """
    if columns is None:
        columns = ["id INT", "name STRING", "age INT"]

    column_defs = ",\n  ".join(columns)
    full_table_name = f"{catalog}.{schema}.{table_name}"

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
      {column_defs}
    )
    TBLPROPERTIES (
      '{DELTA_ENABLE_CDF}' = 'true',
      'comment' = 'Bronze table with raw validated data from API'
    )
    """

    try:
        spark.sql(ddl)
    except Exception as e:
        raise


def create_quarantine_table(
    spark,
    catalog: str,
    schema: str,
    table_name: str,
    data_columns: List[str] = None
) -> None:
    """
    Create Quarantine table with quality tracking columns.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table_name: Table name
        data_columns: List of data column definitions (default: id INT, name STRING, age INT)

    Example:
        >>> create_quarantine_table(spark, "chlor", "dp_ingestion_one_flow", "quarantine_table")
    """
    if data_columns is None:
        data_columns = ["id INT", "name STRING", "age INT"]

    data_col_defs = ",\n  ".join(data_columns)
    full_table_name = f"{catalog}.{schema}.{table_name}"

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
      {data_col_defs},
      quarantine_timestamp TIMESTAMP,
      quarantine_expiry TIMESTAMP,
      quarantine_status STRING,
      validation_errors ARRAY<STRING>,
      quarantine_reason STRING,
      fix_attempts INT,
      source_file STRING,
      ingestion_timestamp TIMESTAMP
    )
    TBLPROPERTIES (
      'comment' = 'Quarantine table for records with quality issues'
    )
    """

    try:
        spark.sql(ddl)
    except Exception as e:
        raise


def create_dead_letter_table(
    spark,
    catalog: str,
    schema: str,
    table_name: str,
    data_columns: List[str] = None
) -> None:
    """
    Create Dead Letter table for expired quarantine records.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table_name: Table name
        data_columns: List of data column definitions

    Example:
        >>> create_dead_letter_table(spark, "chlor", "dp_ingestion_one_flow", "dead_letter_table")
    """
    if data_columns is None:
        data_columns = ["id INT", "name STRING", "age INT"]

    data_col_defs = ",\n  ".join(data_columns)
    full_table_name = f"{catalog}.{schema}.{table_name}"

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
      {data_col_defs},
      quarantine_timestamp TIMESTAMP,
      quarantine_expiry TIMESTAMP,
      quarantine_status STRING,
      validation_errors ARRAY<STRING>,
      quarantine_reason STRING,
      fix_attempts INT,
      source_file STRING,
      ingestion_timestamp TIMESTAMP,
      moved_to_dead_letter TIMESTAMP
    )
    TBLPROPERTIES (
      'comment' = 'Dead letter table for expired quarantine records'
    )
    """

    try:
        spark.sql(ddl)
    except Exception as e:
        raise


def create_silver_table(
    spark,
    catalog: str,
    schema: str,
    table_name: str,
    data_columns: List[str] = None
) -> None:
    """
    Create Silver table for SCD Type 2 tracking.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        table_name: Table name
        data_columns: List of data column definitions

    Example:
        >>> create_silver_table(spark, "chlor", "dp_ingestion_one_flow", "silver_table")
    """
    if data_columns is None:
        data_columns = ["id INT", "name STRING", "age INT"]

    data_col_defs = ",\n  ".join(data_columns)
    full_table_name = f"{catalog}.{schema}.{table_name}"

    ddl = f"""
    CREATE TABLE IF NOT EXISTS {full_table_name} (
      {data_col_defs},
      valid_from TIMESTAMP,
      valid_to TIMESTAMP,
      is_current BOOLEAN
    )
    TBLPROPERTIES (
      'comment' = 'Silver table with SCD Type 2 historical tracking'
    )
    """

    try:
        spark.sql(ddl)
    except Exception as e:
        raise


def create_all_tables(
    spark,
    catalog: str,
    schema: str,
    bronze_table: str,
    quarantine_table: str,
    dead_letter_table: str,
    silver_table: str = None,
    data_columns: List[str] = None
) -> None:
    """
    Create all pipeline tables at once.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        bronze_table: Bronze table name
        quarantine_table: Quarantine table name
        dead_letter_table: Dead letter table name
        silver_table: Silver table name (optional)
        data_columns: List of data column definitions

    Example:
        >>> from src.utils.config import load_config
        >>> config = load_config()
        >>> create_all_tables(
        ...     spark,
        ...     config.catalog,
        ...     config.schema,
        ...     config.bronze_table,
        ...     config.quarantine_table,
        ...     config.dead_letter_table,
        ...     config.silver_table
        ... )
    """

    create_bronze_table(spark, catalog, schema, bronze_table, data_columns)
    create_quarantine_table(spark, catalog, schema, quarantine_table, data_columns)
    create_dead_letter_table(spark, catalog, schema, dead_letter_table, data_columns)

    if silver_table:
        create_silver_table(spark, catalog, schema, silver_table, data_columns)

