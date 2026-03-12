"""Generate synthetic test data for multi-stage pipeline testing."""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "chlor"
SCHEMA = "Pharos"

# Volume paths for each dataset
# Use timestamps that will be latest: 210000, 220000, 230000, 240000 (9pm, 10pm, 11pm, midnight)
FULL_LOAD_1 = f"/Volumes/{CATALOG}/{SCHEMA}/parquet_full/20260311_210000"
PARTIAL_LOAD_1 = f"/Volumes/{CATALOG}/{SCHEMA}/parquet_partial/20260311_220000"
FULL_LOAD_2 = f"/Volumes/{CATALOG}/{SCHEMA}/parquet_full/20260311_230000"
PARTIAL_LOAD_2 = f"/Volumes/{CATALOG}/{SCHEMA}/parquet_partial/20260311_240000"

# =============================================================================
# SETUP
# =============================================================================
spark = SparkSession.builder.getOrCreate()

# Define schema with nullable age column
# Use LongType for id to match Databricks default
from pyspark.sql.types import LongType

schema = StructType([
    StructField("id", LongType(), False),  # Changed to LongType
    StructField("name", StringType(), False),
    StructField("age", LongType(), True)  # Nullable, also LongType
])

print("=" * 60)
print("GENERATING TEST DATASETS FOR MULTI-STAGE PIPELINE TEST")
print("=" * 60)

# =============================================================================
# DATASET 1: Full Load #1 (Initial load with quality issues)
# =============================================================================
print("\n📂 Dataset 1: Full Load #1")
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

# =============================================================================
# DATASET 2: Partial Load #1 (Updates, fixes, new quarantine)
# =============================================================================
print("\n📂 Dataset 2: Partial Load #1")
print(f"   Path: {PARTIAL_LOAD_1}")

dataset2 = [
    (1, "Alice", 31),     # Update existing (age 30 -> 31)
    (2, "Bob", 35),       # Fix quarantine (null -> 35)
    (6, "Frank", None),   # New quarantine - null age
    (7, "Grace", 27),     # New clean record
]

df2 = spark.createDataFrame(dataset2, schema=schema)
df2.write.mode("overwrite").parquet(PARTIAL_LOAD_1)

print(f"   ✅ Created {df2.count()} records")
print("   Data:")
df2.show(truncate=False)

# =============================================================================
# DATASET 3: Full Load #2 (Fresh dataset after quarantine fix)
# =============================================================================
print("\n📂 Dataset 3: Full Load #2")
print(f"   Path: {FULL_LOAD_2}")

dataset3 = [
    (1, "Alice", 32),     # Changed from previous
    (3, "Charlie", 26),   # Changed (25 -> 26)
    (5, "Eve", 29),       # Changed (28 -> 29)
    (8, "Henry", None),   # New quarantine - null age
    (9, "Iris", 24),      # New clean record
]

df3 = spark.createDataFrame(dataset3, schema=schema)
df3.write.mode("overwrite").parquet(FULL_LOAD_2)

print(f"   ✅ Created {df3.count()} records")
print("   Data:")
df3.show(truncate=False)

# =============================================================================
# DATASET 4: Partial Load #2 (More updates and fixes)
# =============================================================================
print("\n📂 Dataset 4: Partial Load #2")
print(f"   Path: {PARTIAL_LOAD_2}")

dataset4 = [
    (1, "Alice", 33),     # Update (32 -> 33)
    (8, "Henry", 40),     # Fix quarantine (null -> 40)
    (10, "Jack", None),   # New quarantine - null age
    (11, "Kate", 26),     # New clean record
]

df4 = spark.createDataFrame(dataset4, schema=schema)
df4.write.mode("overwrite").parquet(PARTIAL_LOAD_2)

print(f"   ✅ Created {df4.count()} records")
print("   Data:")
df4.show(truncate=False)

# =============================================================================
# SUMMARY
# =============================================================================
print("\n" + "=" * 60)
print("SUMMARY: All datasets created successfully")
print("=" * 60)
print("\n📊 Dataset Overview:")
print(f"   Full Load #1:     5 records (2 with null age)")
print(f"   Partial Load #1:  4 records (1 with null age, 1 fix)")
print(f"   Full Load #2:     5 records (1 with null age)")
print(f"   Partial Load #2:  4 records (1 with null age, 1 fix)")
print("\n✅ Ready for multi-stage pipeline testing!")
