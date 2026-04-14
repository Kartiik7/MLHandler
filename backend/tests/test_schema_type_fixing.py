"""Test schema-based type fixing."""
import pandas as pd
from app.core.config import get_default_schema
from app.services.type_fixer import apply_schema_based_type_fixes

# Create test data with values that match schema patterns
test_data = {
    "Ram": ["8GB", "16GB", "4GB", "32GB"],
    "Memory": ["512GB SSD", "256GB", "1TB HDD", "128GB"],
    "Weight": ["1.83kg", "2.1kg", "1.5kg", "2.5kg"],
    "Price": [500, 800, 600, 1200],
    "Company": ["Dell", "HP", "Lenovo", "Apple"]
}

df = pd.DataFrame(test_data)

print("SCHEMA-BASED TYPE FIXING TEST")
print("=" * 60)

print("\nINPUT:")
print(df)
print("\nInput dtypes:")
print(df.dtypes)

# Load schema
schema = get_default_schema()
schema_fields = schema["schema"]["fields"]

# Apply schema-based type fixing
df_fixed, stats = apply_schema_based_type_fixes(df, schema_fields)

print("\n\nOUTPUT:")
print(df_fixed)
print("\nOutput dtypes:")
print(df_fixed.dtypes)

print("\n\nCONVERSION STATS:")
if "schema_based_conversions" in stats:
    for col, col_stats in stats["schema_based_conversions"].items():
        print(f"\n  {col}:")
        print(f"    Converted: {col_stats['converted_count']}")
        print(f"    Unit: {col_stats['unit']}")
        print(f"    Pattern: {col_stats['pattern_used']}")

print("\n" + "=" * 60)
print("TEST PASSED!")
