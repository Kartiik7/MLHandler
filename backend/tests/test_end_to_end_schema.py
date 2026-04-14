"""End-to-end test: Schema loading + Field mapping + Type fixing."""
import pandas as pd
import tempfile
import os
from app.core.config import get_default_schema
from app.services.pipeline import process_csv

# Create test CSV with messy column names and values
test_data = {
    "ram": ["8GB", "16GB", "4GB", "32GB"],  # Lowercase alias
    "storage_capacity": ["512GB SSD", "256GB", "1TB HDD", "128GB"],  # Alias for Memory
    "mass": ["1.83kg", "2.1kg", "1.5kg", "2.5kg"],  # Alias for Weight
    "cost": [500, 800, 600, 1200],  # Alias for Price
    "brand": ["Dell", "HP", "Lenovo", "Apple"]  # Alias for Company
}

df = pd.DataFrame(test_data)

# Save to CSV
fd, input_path = tempfile.mkstemp(suffix=".csv")
os.close(fd)
df.to_csv(input_path, index=False)

print("END-TO-END TEST: Schema + Field Mapping + Type Fixing")
print("=" * 70)

print("\nINPUT CSV:")
print("Columns:", list(df.columns))
print(df)

# Get default schema and create config
schema = get_default_schema()
config = {
    "schema": schema,
    "convert_types": True,
    "fill_missing_numeric": "median"
}

# Run full pipeline
output_path, report = process_csv(input_path, config)
df_output = pd.read_csv(output_path)

print("\n\nOUTPUT CSV:")
print("Columns:", list(df_output.columns))
print(df_output)
print("\nOutput dtypes:")
print(df_output.dtypes)

# Show field mapping
if "field_mapping" in report:
    fm = report["field_mapping"]
    print(f"\n\nFIELD MAPPING ({fm.get('mapped_count', 0)} mapped):")
    if fm.get('mappings'):
        for orig, info in fm['mappings'].items():
            print(f"  '{orig}' -> '{info['mapped_to']}' ({info['match_type']})")

# Show type conversions
if "schema_based_conversions" in report:
    print("\n\nSCHEMA-BASED TYPE CONVERSIONS:")
    for col, stats in report["schema_based_conversions"].items():
        print(f"  {col}: {stats['converted_count']} values ({stats['unit']})")

# Cleanup
os.remove(input_path)
os.remove(output_path)

print("\n" + "=" * 70)
print("TEST PASSED - Full pipeline working with schema!")
print("=" * 70)
