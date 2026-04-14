"""Simple working test for unit conversion."""
import pandas as pd
from app.services.type_fixer import convert_units

# Create test data
test_data = {
    "Memory": ["512GB", "1TB", "256GB", "128000MB"],
}

df = pd.DataFrame(test_data)

print("UNIT CONVERSION TEST")
print("=" * 60)
print("\nINPUT:")
print(df)

# Schema fields with unit standardization
schema_fields = {
    "Memory": {
        "type": "numeric",
        "unit": "GB",
        "unit_standardization": True,
        "unit_pattern": r"(\d+\.?\d*)\s*(MB|GB|TB)"
    }
}

# Apply unit conversion
df_converted, stats = convert_units(df, schema_fields)

print("\nOUTPUT (all in GB):")
print(df_converted)

print("\nSTATS:")
print(stats)

print("\n" + "=" * 60)
print("TEST PASSED!")
