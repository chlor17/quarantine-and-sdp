"""File discovery utilities for landing zone operations."""

import os
from typing import List, Tuple


def get_latest_parquet_path(catalog: str, schema: str, volume_name: str) -> str:
    """
    Get path to latest timestamped parquet folder in a volume.

    Args:
        catalog: Catalog name
        schema: Schema name
        volume_name: Volume name (e.g., 'parquet_full' or 'parquet_partial')

    Returns:
        str: Full path to latest parquet folder

    Raises:
        FileNotFoundError: If volume directory doesn't exist
        ValueError: If no parquet folders found in volume

    Example:
        >>> latest_path = get_latest_parquet_path("your_catalog", "your_schema", "parquet_full")
        >>> print(latest_path)
        /Volumes/your_catalog/your_schema/parquet_full/20260306_141500
    """
    parquet_dir = f"/Volumes/{catalog}/{schema}/{volume_name}"

    # Check if directory exists
    if not os.path.exists(parquet_dir):
        raise FileNotFoundError(
            f"Volume directory not found: {parquet_dir}. "
            f"Please ensure the volume exists in catalog {catalog}, schema {schema}"
        )

    # Get all folders (exclude hidden files starting with '.')
    try:
        folders = [f for f in os.listdir(parquet_dir) if not f.startswith('.')]
    except OSError as e:
        raise FileNotFoundError(f"Could not read directory {parquet_dir}: {e}")

    if not folders:
        raise ValueError(
            f"No parquet folders found in {parquet_dir}. "
            f"Please ensure data has been written to the volume."
        )

    # Get latest folder (assuming timestamped folder names sort lexicographically)
    latest_folder = max(folders)
    latest_path = os.path.join(parquet_dir, latest_folder)

    return latest_path


def get_all_parquet_paths(catalog: str, schema: str, volume_name: str) -> List[str]:
    """
    Get all timestamped parquet folder paths in a volume.

    Args:
        catalog: Catalog name
        schema: Schema name
        volume_name: Volume name

    Returns:
        List[str]: List of parquet folder paths, sorted chronologically

    Raises:
        FileNotFoundError: If volume directory doesn't exist

    Example:
        >>> all_paths = get_all_parquet_paths("your_catalog", "your_schema", "parquet_full")
        >>> for path in all_paths:
        ...     print(path)
    """
    parquet_dir = f"/Volumes/{catalog}/{schema}/{volume_name}"

    if not os.path.exists(parquet_dir):
        raise FileNotFoundError(f"Volume directory not found: {parquet_dir}")

    try:
        folders = sorted([f for f in os.listdir(parquet_dir) if not f.startswith('.')])
    except OSError as e:
        raise FileNotFoundError(f"Could not read directory {parquet_dir}: {e}")

    paths = [os.path.join(parquet_dir, folder) for folder in folders]


    return paths


def get_latest_from_multiple_volumes(
    catalog: str,
    schema: str,
    volume_names: List[str]
) -> List[Tuple[str, str, str]]:
    """
    Get latest parquet paths from multiple volumes.

    Useful for Quality Check process that reads from both full and partial volumes.

    Args:
        catalog: Catalog name
        schema: Schema name
        volume_names: List of volume names to check

    Returns:
        List[Tuple[str, str, str]]: List of (volume_name, latest_folder, full_path) tuples

    Example:
        >>> results = get_latest_from_multiple_volumes(
        ...     "your_catalog", "your_schema",
        ...     ["parquet_full", "parquet_partial"]
        ... )
        >>> for volume, folder, path in results:
        ...     print(f"{volume}: {folder}")
    """
    results = []

    for volume_name in volume_names:
        try:
            latest_path = get_latest_parquet_path(catalog, schema, volume_name)
            latest_folder = os.path.basename(latest_path)
            results.append((volume_name, latest_folder, latest_path))
        except Exception:
            # Skip volumes with no data
            continue

    if not results:
        raise ValueError(
            f"No parquet data found in any of the volumes: {volume_names}"
        )

    return results


def validate_parquet_readable(spark, path: str) -> bool:
    """
    Validate that parquet files at path are readable.

    Args:
        spark: SparkSession
        path: Path to parquet files

    Returns:
        bool: True if readable, False otherwise

    Example:
        >>> if validate_parquet_readable(spark, latest_path):
        ...     df = spark.read.parquet(latest_path)
    """
    try:
        # Try to read schema without loading data
        df = spark.read.parquet(path)
        df.schema  # Access schema to trigger validation
        return True
    except Exception as e:
        return False
