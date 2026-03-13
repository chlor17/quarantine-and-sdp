"""
Quarantine operations for demo_marketing pipeline.

Consolidates logic for:
- Getting active quarantined IDs
- Detecting and releasing fixed records
- Moving expired records to Dead Letter table
"""

from typing import List
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.utils import AnalysisException
from delta.tables import DeltaTable

from src.utils.constants import (
    QUARANTINE_STATUS_ACTIVE,
    QUARANTINE_STATUS_QUARANTINE,
    QUARANTINE_STATUS_BLOCKED,
    QUARANTINE_STATUS_FIXED,
    QUARANTINE_STATUS_RELEASED,
    QUARANTINE_STATUS_EXPIRED
)



def get_active_quarantined_ids(
    spark,
    catalog: str,
    schema: str,
    quarantine_table: str
) -> List[int]:
    """
    Get list of IDs currently in active quarantine.

    Returns IDs that are either:
    - QUARANTINE status (data quality issues)
    - BLOCKED status (valid data blocked by ID stacking)
    - ACTIVE status (legacy compatibility)

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        quarantine_table: Quarantine table name

    Returns:
        List[int]: List of quarantined IDs

    Example:
        >>> quarantined_ids = get_active_quarantined_ids(spark, "chlor", "dp_ingestion_one_flow", "quarantine_table")
        >>> print(f"Found {len(quarantined_ids)} quarantined IDs")
    """
    full_table_name = f"{catalog}.{schema}.{quarantine_table}"

    try:
        quarantined_ids_df = spark.table(full_table_name) \
            .filter(
                (F.col('quarantine_status') == QUARANTINE_STATUS_QUARANTINE) |
                (F.col('quarantine_status') == QUARANTINE_STATUS_BLOCKED) |
                (F.col('quarantine_status') == QUARANTINE_STATUS_ACTIVE)  # Legacy
            ) \
            .select('id').distinct()

        quarantined_ids = [row.id for row in quarantined_ids_df.collect()]

        return quarantined_ids

    except AnalysisException as e:
        if "Table or view not found" in str(e):
            return []
        else:
            raise

    except Exception as e:
        raise


def detect_and_release_fixed_records(
    spark,
    catalog: str,
    schema: str,
    quarantine_table: str,
    bronze_table: str,
    required_columns: List[str]
) -> int:
    """
    Detect fixed records in quarantine and release them to Bronze table.

    A record is considered "fixed" if all required columns are non-null.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        quarantine_table: Quarantine table name
        bronze_table: Bronze table name
        required_columns: List of required column names

    Returns:
        int: Number of records released to Bronze

    Example:
        >>> released_count = detect_and_release_fixed_records(
        ...     spark, "chlor", "dp_ingestion_one_flow",
        ...     "quarantine_table", "bronze_table",
        ...     ["id", "name", "age"]
        ... )
        >>> print(f"Released {released_count} fixed records")
    """
    quarantine_full_name = f"{catalog}.{schema}.{quarantine_table}"
    bronze_full_name = f"{catalog}.{schema}.{bronze_table}"

    try:
        # Get active quarantine records
        active_quarantine = spark.table(quarantine_full_name) \
            .filter(F.col('quarantine_status') == QUARANTINE_STATUS_ACTIVE)

        # Build condition for all columns being non-null
        null_check_conditions = [F.col(col).isNotNull() for col in required_columns]
        combined_condition = null_check_conditions[0]
        for condition in null_check_conditions[1:]:
            combined_condition = combined_condition & condition

        # Find records that are now valid
        fixed_records = active_quarantine.filter(combined_condition)
        fixed_count = fixed_records.count()

        if fixed_count == 0:
            return 0


        # Get unique IDs that are fixed
        fixed_ids = [row.id for row in fixed_records.select('id').distinct().collect()]

        # Mark ALL records with these IDs as FIXED
        delta_quarantine = DeltaTable.forName(spark, quarantine_full_name)
        delta_quarantine.update(
            condition=(F.col('id').isin(fixed_ids)) &
                      (F.col('quarantine_status') == QUARANTINE_STATUS_ACTIVE),
            set={
                'quarantine_status': F.lit(QUARANTINE_STATUS_FIXED),
                'fix_attempts': F.col('fix_attempts') + 1
            }
        )


        # Get the fixed records to move to Bronze (latest version per ID)
        window_spec = Window.partitionBy('id').orderBy(F.desc('quarantine_timestamp'))

        records_to_bronze = spark.table(quarantine_full_name) \
            .filter(F.col('quarantine_status') == QUARANTINE_STATUS_FIXED) \
            .withColumn('rn', F.row_number().over(window_spec)) \
            .filter(F.col('rn') == 1) \
            .select(*required_columns)

        # Write to Bronze table
        records_to_bronze.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(bronze_full_name)

        released_count = records_to_bronze.count()

        # Mark as RELEASED
        delta_quarantine.update(
            condition=F.col('quarantine_status') == QUARANTINE_STATUS_FIXED,
            set={'quarantine_status': F.lit(QUARANTINE_STATUS_RELEASED)}
        )

        return released_count

    except AnalysisException as e:
        if "Table or view not found" in str(e):
            return 0
        else:
            raise

    except Exception as e:
        raise


def move_expired_to_dead_letter(
    spark,
    catalog: str,
    schema: str,
    quarantine_table: str,
    dead_letter_table: str
) -> int:
    """
    Move expired quarantine records to Dead Letter table.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        quarantine_table: Quarantine table name
        dead_letter_table: Dead letter table name

    Returns:
        int: Number of records moved to Dead Letter

    Example:
        >>> expired_count = move_expired_to_dead_letter(
        ...     spark, "chlor", "dp_ingestion_one_flow",
        ...     "quarantine_table", "dead_letter_table"
        ... )
        >>> print(f"Moved {expired_count} expired records")
    """
    quarantine_full_name = f"{catalog}.{schema}.{quarantine_table}"
    dead_letter_full_name = f"{catalog}.{schema}.{dead_letter_table}"

    try:
        # Find expired active records
        expired_records = spark.table(quarantine_full_name) \
            .filter(F.col('quarantine_status') == QUARANTINE_STATUS_ACTIVE) \
            .filter(F.col('quarantine_expiry') < F.current_timestamp())

        expired_count = expired_records.count()

        if expired_count == 0:
            return 0


        # Move to Dead Letter table
        expired_with_timestamp = expired_records.withColumn(
            'moved_to_dead_letter',
            F.current_timestamp()
        )

        expired_with_timestamp.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(dead_letter_full_name)


        # Mark as EXPIRED in quarantine
        delta_quarantine = DeltaTable.forName(spark, quarantine_full_name)
        delta_quarantine.update(
            condition=(F.col('quarantine_expiry') < F.current_timestamp()) &
                      (F.col('quarantine_status') == QUARANTINE_STATUS_ACTIVE),
            set={'quarantine_status': F.lit(QUARANTINE_STATUS_EXPIRED)}
        )


        # Log expired records summary
        expired_summary = expired_records.groupBy('quarantine_reason') \
            .agg(F.count('*').alias('count')) \
            .orderBy(F.desc('count')) \
            .collect()

        return expired_count

    except AnalysisException as e:
        if "Table or view not found" in str(e):
            return 0
        else:
            raise

    except Exception as e:
        raise


def promote_blocked_records(
    spark,
    catalog: str,
    schema: str,
    quarantine_table: str,
    bronze_table: str,
    required_columns: List[str]
) -> int:
    """
    Promote BLOCKED records to Bronze when blocking condition clears.

    A BLOCKED record can be promoted if:
    - Its status is BLOCKED (valid data blocked by ID stacking)
    - The original quarantined ID has been RELEASED or FIXED

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        quarantine_table: Quarantine table name
        bronze_table: Bronze table name
        required_columns: List of required column names

    Returns:
        int: Number of BLOCKED records promoted to Bronze

    Example:
        >>> promoted_count = promote_blocked_records(
        ...     spark, "your_catalog", "your_schema",
        ...     "quarantine_table", "bronze_table",
        ...     ["id", "name", "age"]
        ... )
        >>> print(f"Promoted {promoted_count} blocked records")
    """
    quarantine_full_name = f"{catalog}.{schema}.{quarantine_table}"
    bronze_full_name = f"{catalog}.{schema}.{bronze_table}"

    try:
        # Get currently active quarantined IDs (QUARANTINE status only)
        active_quarantine_ids = spark.table(quarantine_full_name) \
            .filter(F.col('quarantine_status') == QUARANTINE_STATUS_QUARANTINE) \
            .select('id').distinct()

        quarantine_ids = [row.id for row in active_quarantine_ids.collect()]

        # Find BLOCKED records whose blocking ID is no longer quarantined
        blocked_records = spark.table(quarantine_full_name) \
            .filter(F.col('quarantine_status') == QUARANTINE_STATUS_BLOCKED) \
            .filter(~F.col('id').isin(quarantine_ids))  # ID is no longer quarantined

        blocked_count = blocked_records.count()

        if blocked_count == 0:
            return 0

        # Get unique IDs that can be promoted
        blocked_ids = [row.id for row in blocked_records.select('id').distinct().collect()]

        # Get latest version per ID (using window function)
        window_spec = Window.partitionBy('id').orderBy(F.desc('quarantine_timestamp'))

        records_to_bronze = spark.table(quarantine_full_name) \
            .filter(F.col('id').isin(blocked_ids)) \
            .filter(F.col('quarantine_status') == QUARANTINE_STATUS_BLOCKED) \
            .withColumn('rn', F.row_number().over(window_spec)) \
            .filter(F.col('rn') == 1) \
            .select(*required_columns)

        # Write to Bronze table
        records_to_bronze.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable(bronze_full_name)

        promoted_count = records_to_bronze.count()

        # Mark as RELEASED
        delta_quarantine = DeltaTable.forName(spark, quarantine_full_name)
        delta_quarantine.update(
            condition=(F.col('id').isin(blocked_ids)) &
                      (F.col('quarantine_status') == QUARANTINE_STATUS_BLOCKED),
            set={'quarantine_status': F.lit(QUARANTINE_STATUS_RELEASED)}
        )

        return promoted_count

    except AnalysisException as e:
        if "Table or view not found" in str(e):
            return 0
        else:
            raise

    except Exception as e:
        raise


def get_quarantine_health_stats(
    spark,
    catalog: str,
    schema: str,
    quarantine_table: str
) -> dict:
    """
    Get health statistics for quarantine table.

    Args:
        spark: SparkSession
        catalog: Catalog name
        schema: Schema name
        quarantine_table: Quarantine table name

    Returns:
        dict: Dictionary with status counts and health metrics

    Example:
        >>> stats = get_quarantine_health_stats(spark, "chlor", "dp_ingestion_one_flow", "quarantine_table")
        >>> print(f"Active: {stats['active']}, Fixed: {stats['fixed']}, Expired: {stats['expired']}")
    """
    full_table_name = f"{catalog}.{schema}.{quarantine_table}"

    try:
        status_counts = spark.sql(f"""
            SELECT
                quarantine_status,
                COUNT(*) as count,
                COUNT(DISTINCT id) as unique_ids
            FROM {full_table_name}
            GROUP BY quarantine_status
        """).collect()

        stats = {
            'active': 0,  # Legacy
            'quarantine': 0,
            'blocked': 0,
            'fixed': 0,
            'released': 0,
            'expired': 0,
            'total': 0
        }

        for row in status_counts:
            status = row.quarantine_status.lower()
            count = row['count']
            stats[status] = count
            stats['total'] += count

        return stats

    except AnalysisException as e:
        if "Table or view not found" in str(e):
            return {'active': 0, 'fixed': 0, 'released': 0, 'expired': 0, 'total': 0}
        else:
            raise

    except Exception as e:
        raise
