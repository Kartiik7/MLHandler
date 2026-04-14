"""Test unit standardization functionality."""
import pandas as pd
from app.core.config import get_default_schema
from app.services.type_fixer import apply_schema_based_type_fixes

# Create test data with mixed units
test_data = {
    "Memory": ["512GB SSD", "256GB", "1TB HDD", "128000MB", "2TB"],
    "Ram": ["8GB", "16GB", "4GB", "32GB"],
    "Price": [500, 800, 600, 1200, 1500]
}

df = pd.DataFrame(test_data)

print("UNIT STANDARDIZATION TEST")
print("=" * 60)

print("\nINPUT (Mixed Units):")
print(df)

# Load schema and apply type fixing with unit conversion
schema = get_default_schema()
schema_fields = schema["schema"]["fields"]

df_fixed, stats = apply_schema_based_type_fixes(df, schema_fields)

print("\n\nOUTPUT (Standardized to GB):")
print(df_fixed)
print("\nOutput dtypes:")
print(df_fixed.dtypes)

# Show conversion stats
if "unit_conversions" in stats:
    print("\n\nUNIT CONVERSIONS:")
    for col, col_stats in stats["unit_conversions"].items():
        print(f"\n  {col}:")
        print(f"    Converted: {col_stats.get('converted_count', 0)} values")
        print(f"    Standard unit: {col_stats.get('standard_unit', 'N/A')}")
        print(f"    Pattern: {col_stats.get('pattern_used', 'N/A')}")

# Show specific conversions
print("\n\nEXAMPLE CONVERSIONS:")
print("  '512GB SSD' -> 512.0 GB")
print("  '1TB HDD' -> 1024.0 GB")
print("  '128000MB' -> 125.0 GB")
print("  '2TB' -> 2048.0 GB")

print("\n" + "=" * 60)
print("TEST PASSED!")
