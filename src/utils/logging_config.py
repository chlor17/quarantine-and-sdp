"""
Logging configuration for demo_marketing pipeline.

Provides structured logging with timestamps and context tracking.
Replaces print() statements with proper logging levels.
"""

import logging
import sys
from typing import Optional
from datetime import datetime


class StructuredFormatter(logging.Formatter):
    """Custom formatter with timestamps and structured output."""

    def format(self, record):
        """Format log record with timestamp and level."""
        timestamp = datetime.fromtimestamp(record.created).strftime('%Y-%m-%d %H:%M:%S')

        # Color codes for terminal output (optional)
        level_colors = {
            'DEBUG': '\033[36m',      # Cyan
            'INFO': '\033[32m',       # Green
            'WARNING': '\033[33m',    # Yellow
            'ERROR': '\033[31m',      # Red
            'CRITICAL': '\033[35m',   # Magenta
        }
        reset = '\033[0m'

        level_name = record.levelname
        color = level_colors.get(level_name, '')

        # Format: [timestamp] LEVEL | module | message
        formatted = f"[{timestamp}] {color}{level_name:8s}{reset} | {record.name:20s} | {record.getMessage()}"

        # Add exception info if present
        if record.exc_info:
            formatted += '\n' + self.formatException(record.exc_info)

        return formatted


def setup_logger(
    name: str,
    level: str = "INFO",
    use_colors: bool = True
) -> logging.Logger:
    """
    Set up structured logger for pipeline components.

    Args:
        name: Logger name (typically __name__ from calling module)
        level: Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        use_colors: Whether to use colored output (default True)

    Returns:
        logging.Logger: Configured logger instance

    Example:
        >>> from src.utils.logging_config import setup_logger
        >>> logger = setup_logger(__name__)
        >>> logger.info("Starting full load process", extra={"catalog": "chlor"})
        >>> logger.warning("Found quality issues", extra={"count": 5})
        >>> logger.error("Failed to process", exc_info=True)
    """
    # Create logger
    logger = logging.getLogger(name)

    # Avoid duplicate handlers
    if logger.handlers:
        return logger

    # Set level
    numeric_level = getattr(logging, level.upper(), logging.INFO)
    logger.setLevel(numeric_level)

    # Create console handler
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(numeric_level)

    # Set formatter
    if use_colors:
        formatter = StructuredFormatter()
    else:
        formatter = logging.Formatter(
            '[%(asctime)s] %(levelname)-8s | %(name)-20s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )

    handler.setFormatter(formatter)
    logger.addHandler(handler)

    # Don't propagate to root logger (avoid duplicate logs)
    logger.propagate = False

    return logger


def log_dataframe_info(logger: logging.Logger, df, description: str = "DataFrame"):
    """
    Log DataFrame schema and count information.

    Args:
        logger: Logger instance
        df: PySpark DataFrame
        description: Description of the DataFrame

    Example:
        >>> logger = setup_logger(__name__)
        >>> log_dataframe_info(logger, bronze_df, "Bronze table")
    """
    try:
        count = df.count()
        columns = df.columns
        logger.info(
            f"{description} - Rows: {count}, Columns: {len(columns)} {columns}"
        )
    except Exception as e:
        logger.warning(f"Could not log DataFrame info for {description}: {e}")


def log_operation_summary(
    logger: logging.Logger,
    operation: str,
    success: bool,
    records_processed: int = 0,
    records_clean: int = 0,
    records_quarantine: int = 0,
    duration_seconds: Optional[float] = None
):
    """
    Log summary of pipeline operation.

    Args:
        logger: Logger instance
        operation: Operation name (e.g., "Full Load", "Quality Check")
        success: Whether operation succeeded
        records_processed: Total records processed
        records_clean: Clean records
        records_quarantine: Quarantined records
        duration_seconds: Operation duration

    Example:
        >>> import time
        >>> start = time.time()
        >>> # ... do work ...
        >>> duration = time.time() - start
        >>> log_operation_summary(
        ...     logger, "Quality Check", True,
        ...     records_processed=100, records_clean=95, records_quarantine=5,
        ...     duration_seconds=duration
        ... )
    """
    status = "SUCCESS" if success else "FAILED"
    message = f"Operation: {operation} | Status: {status}"

    if records_processed > 0:
        message += f" | Processed: {records_processed}"
    if records_clean > 0:
        message += f" | Clean: {records_clean}"
    if records_quarantine > 0:
        message += f" | Quarantine: {records_quarantine}"
    if duration_seconds is not None:
        message += f" | Duration: {duration_seconds:.2f}s"

    if success:
        logger.info(message)
    else:
        logger.error(message)
