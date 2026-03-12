"""Generate Full Load #2 test data."""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "chlor"
SCHEMA = "Pharos"
FULL_LOAD_2 = f"/Volumes/{CATALOG}/{SCHEMA}/parquet_full/20991231_030000"

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
print("GENERATING FULL LOAD #2")
print("=" * 60)

# =============================================================================
# Full Load #2: Fresh dataset
# =============================================================================
print(f"\n📂 Full Load #2")
print(f"   Path: {FULL_LOAD_2}")

dataset = [
    (1, "Alice", 32),     # Changed from previous
    (3, "Charlie", 26),   # Changed (25 -> 26)
    (5, "Eve", 29),       # Changed (28 -> 29)
    (8, "Henry", None),   # New quarantine
    (9, "Iris", 24),      # New clean
]

df = spark.createDataFrame(dataset, schema=schema)
df.write.mode("overwrite").parquet(FULL_LOAD_2)

print(f"   ✅ Created {df.count()} records")
print("   Data:")
df.show(truncate=False)

print("\n✅ Full Load #2 ready!")
print(f"   Clean: 4 records (Alice, Charlie, Eve, Iris)")
print(f"   Quarantine: 1 record (Henry - null age)")
