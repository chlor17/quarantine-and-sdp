"""Generate Full Load #1 test data only."""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "your_catalog"
SCHEMA = "your_schema"
FULL_LOAD_1 = f"/Volumes/{CATALOG}/{SCHEMA}/parquet_full/20991231_010000"

# =============================================================================
# SETUP
# =============================================================================
spark = SparkSession.builder.getOrCreate()

# Define schema with nullable age column
schema = StructType([
    StructField("id", LongType(), False),
    StructField("name", StringType(), False),
    StructField("age", LongType(), True)  # Nullable
])

print("=" * 60)
print("GENERATING FULL LOAD #1")
print("=" * 60)

# =============================================================================
# DATASET 1: Full Load #1 (Initial load with quality issues)
# =============================================================================
print(f"\n📂 Full Load #1")
print(f"   Path: {FULL_LOAD_1}")

dataset1 = [
    (1, "Alice", 30),
    (2, "Bob", None),     # Quarantine - null age
    (3, "Charlie", 25),
    (4, "Diana", None),   # Quarantine - null age
    (5, "Eve", 28),
]

df1 = spark.createDataFrame(dataset1, schema=schema)
df1.write.mode("overwrite").parquet(FULL_LOAD_1)

print(f"   ✅ Created {df1.count()} records")
print("   Data:")
df1.show(truncate=False)

print("\n✅ Full Load #1 ready!")
print(f"   Clean records: 3 (Alice, Charlie, Eve)")
print(f"   Quarantine records: 2 (Bob, Diana - null age)")
