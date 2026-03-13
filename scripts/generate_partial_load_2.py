"""Generate Partial Load #2 test data."""
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, LongType, StringType

# =============================================================================
# CONFIGURATION
# =============================================================================
CATALOG = "your_catalog"
SCHEMA = "your_schema"
PARTIAL_LOAD_2 = f"/Volumes/{CATALOG}/{SCHEMA}/parquet_partial/20991231_040000"

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
print("GENERATING PARTIAL LOAD #2")
print("=" * 60)

# =============================================================================
# Partial Load #2: More updates and fixes
# =============================================================================
print(f"\n📂 Partial Load #2")
print(f"   Path: {PARTIAL_LOAD_2}")

dataset = [
    (1, "Alice", 33),      # Update (32 -> 33)
    (8, "Henry", 40),      # Fix quarantine (null -> 40)
    (10, "Jack", None),    # New quarantine
    (11, "Kate", 26),      # New clean
]

df = spark.createDataFrame(dataset, schema=schema)
df.write.mode("overwrite").parquet(PARTIAL_LOAD_2)

print(f"   ✅ Created {df.count()} records")
print("   Data:")
df.show(truncate=False)

print("\n✅ Partial Load #2 ready!")
print(f"   Updates: Alice 32→33")
print(f"   Fixes: Henry (null→40)")
print(f"   New quarantine: Jack (null age)")
print(f"   New clean: Kate (26)")
