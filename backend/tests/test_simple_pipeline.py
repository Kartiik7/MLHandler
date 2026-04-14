"""Simple test for pipeline integration with FieldMapper."""
import pandas as pd
import tempfile
import os
from app.services.pipeline import process_csv

# Create test CSV
test_data = {
    "ram": ["8GB", "16GB", "4GB", "32GB"],
    "WEIGHT": ["1.83kg", "2.1kg", "1.5kg", "2.5kg"],
    "Company": ["Dell", "HP", "Lenovo", "Apple"],
    "product_price": [500, 800, 600, 1200],
}

df = pd.DataFrame(test_data)

# Save to CSV
fd, input_path = tempfile.mkstemp(suffix=".csv")
os.close(fd)
df.to_csv(input_path, index=False)

# Define schema
schema = {
    "fields": {
        "Ram": {
            "aliases": ["ram", "memory_ram", "RAM"],
            "type": "numeric"
        },
        "Weight": {
            "aliases": ["weight", "WEIGHT"],
            "type": "numeric"
        },
        "Company": {
            "aliases": ["company", "brand"],
            "type": "categorical"
        },
        "Price": {
            "aliases": ["price", "product_price"],
            "type": "numeric"
        }
    }
}

config = {"schema": schema, "convert_types": True}

print("INPUT COLUMNS:", list(df.columns))

# Run pipeline
output_path, report = process_csv(input_path, config)
df_output = pd.read_csv(output_path)

print("OUTPUT COLUMNS:", list(df_output.columns))
print("OUTPUT DTYPES:", dict(df_output.dtypes))

# Check field mapping
if "field_mapping" in report:
    fm = report["field_mapping"]
    print("\nFIELD MAPPING:")
    print(f"  Mapped: {fm.get('mapped_count', 0)}")
    print(f"  Unmapped: {fm.get('unmapped_count', 0)}")
    if fm.get('mappings'):
        for orig, info in fm['mappings'].items():
            print(f"  '{orig}' -> '{info['mapped_to']}' ({info['match_type']})")

# Check type conversions
if "type_stats" in report and "rule_based_conversions" in report["type_stats"]:
    print("\nTYPE CONVERSIONS:")
    for col, stats in report["type_stats"]["rule_based_conversions"].items():
        print(f"  {col}: {stats['converted_count']} converted")

# Cleanup
os.remove(input_path)
os.remove(output_path)

print("\nTEST PASSED!")
