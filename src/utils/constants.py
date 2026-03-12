"""
Constants for demo_marketing pipeline.

Centralizes all magic strings and constants used across the pipeline.
"""

# Quarantine status constants
QUARANTINE_STATUS_ACTIVE = "ACTIVE"  # Legacy - kept for compatibility
QUARANTINE_STATUS_QUARANTINE = "QUARANTINE"  # Data quality issues
QUARANTINE_STATUS_BLOCKED = "BLOCKED"  # Valid data blocked by ID stacking
QUARANTINE_STATUS_FIXED = "FIXED"
QUARANTINE_STATUS_RELEASED = "RELEASED"
QUARANTINE_STATUS_EXPIRED = "EXPIRED"

# Validation error codes
ERROR_NULL_ID = "null_id"
ERROR_NULL_NAME = "null_name"
ERROR_NULL_AGE = "null_age"

# Default configuration values
DEFAULT_QUARANTINE_HOURS = 24
DEFAULT_REQUIRED_COLUMNS = ["id", "name", "age"]

# Table properties
DELTA_ENABLE_CDF = "delta.enableChangeDataFeed"

# Log levels
LOG_LEVEL_DEBUG = "DEBUG"
LOG_LEVEL_INFO = "INFO"
LOG_LEVEL_WARNING = "WARNING"
LOG_LEVEL_ERROR = "ERROR"
