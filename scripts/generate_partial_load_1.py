"""Generate Partial Load #1 test data."""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "chlor"
SCHEMA = "Pharos"
PARTIAL_LOAD_1 = f"/Volumes/{CATALOG}/{SCHEMA}/parquet_partial/20991231_020000"

# =============================================================================
# SETUP
# =============================================================================
spark = SparkSession.builder.getOrCreate()

schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), False),
    StructField("age", LongType(), True)
])

print("=" * 60)
print("GENERATING PARTIAL LOAD #1")
print("=" * 60)

# =============================================================================
# Partial Load #1: Updates, fixes, new quarantine
# =============================================================================
print(f"\n📂 Partial Load #1")
print(f"   Path: {PARTIAL_LOAD_1}")

dataset = [
    (1, "Alice", 31),     # Update existing (30 -> 31)
    (2, "Bob", 35),       # Fix quarantine (null -> 35)
    (6, "Frank", None),   # New quarantine
    (7, "Grace", 27),     # New clean
]

df = spark.createDataFrame(dataset, schema=schema)
df.write.mode("overwrite").parquet(PARTIAL_LOAD_1)

print(f"   ✅ Created {df.count()} records")
print("   Data:")
df.show(truncate=False)

print("\n✅ Partial Load #1 ready!")
print(f"   Updates: Alice 30→31")
print(f"   Fixes: Bob (null→35)")
print(f"   New quarantine: Frank (null age)")
print(f"   New clean: Grace (27)")
