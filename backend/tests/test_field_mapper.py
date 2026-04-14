"""Test FieldMapper functionality."""
import pandas as pd
from app.services.field_mapper import FieldMapper

# Define schema with aliases
schema = {
    "fields": {
        "Ram": {
            "aliases": ["ram", "memory_ram", "RAM", "system_memory"],
            "type": "numeric",
            "unit": "GB"
        },
        "Weight": {
            "aliases": ["weight", "mass", "WEIGHT", "product_weight"],
            "type": "numeric",
            "unit": "kg"
        },
        "Company": {
            "aliases": ["company", "manufacturer", "brand", "COMPANY"],
            "type": "categorical"
        },
        "Price": {
            "aliases": ["price", "cost", "PRICE", "product_price"],
            "type": "numeric",
            "unit": "USD"
        }
    }
}

# Create test DataFrame with various column name formats
test_data = {
    "ram": [8, 16, 4, 32],              # Lowercase (alias)
    "WEIGHT": [1.83, 2.1, 1.5, 2.5],    # Uppercase (alias)
    "Company": ["Dell", "HP", "Lenovo", "Apple"],  # Exact match
    "product_price": [500, 800, 600, 1200],  # Alias
    "Screen": [15.6, 14, 13.3, 16],     # Unmapped
    "memory_ram": [16, 32, 8, 64]       # Alias (duplicate - should map to Ram)
}

df = pd.DataFrame(test_data)

print("=" * 60)
print("FIELD MAPPER TEST")
print("=" * 60)

print("\n📊 ORIGINAL DATAFRAME:")
print(df)
print("\nOriginal columns:", list(df.columns))

# Create mapper and map fields
mapper = FieldMapper(schema)

print("\n📋 MAPPING REPORT:")
report = mapper.get_mapping_report(df)
print(f"Total columns: {report['total_columns']}")
print("\nMappings:")
for orig, mapping_info in report['mappings'].items():
    print(f"  '{orig}' → '{mapping_info['mapped_to']}' ({mapping_info['match_type']})")
print(f"\nUnmapped: {report['unmapped']}")

# Apply mapping
df_mapped = mapper.map_fields(df)

print("\n✅ MAPPED DATAFRAME:")
print(df_mapped)
print("\nMapped columns:", list(df_mapped.columns))

print("\n" + "=" * 60)
print("TEST COMPLETE")
print("=" * 60)

# Write to file for easier viewing
with open("field_mapper_test_output.txt", "w") as f:
    f.write("FIELD MAPPER TEST RESULTS\n")
    f.write("=" * 60 + "\n\n")
    f.write("ORIGINAL COLUMNS:\n")
    f.write(str(list(df.columns)) + "\n\n")
    f.write("MAPPED COLUMNS:\n")
    f.write(str(list(df_mapped.columns)) + "\n\n")
    f.write("MAPPING REPORT:\n")
    f.write(str(report) + "\n\n")
    f.write("ORIGINAL DATAFRAME:\n")
    f.write(str(df) + "\n\n")
    f.write("MAPPED DATAFRAME:\n")
    f.write(str(df_mapped) + "\n")

print("\n📄 Full output saved to: field_mapper_test_output.txt")
