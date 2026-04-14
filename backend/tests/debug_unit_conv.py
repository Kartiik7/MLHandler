"""Debug unit conversion issue."""
import pandas as pd

# Simple test
df = pd.DataFrame({"Memory": ["512GB", "1TB", "256GB"]})
print("Original:", df["Memory"].tolist())

# Extract
pattern = r"(\d+\.?\d*)\s*(MB|GB|TB)"
extracted = df["Memory"].astype(str).str.extract(pattern, expand=True)
print("\nExtracted shape:", extracted.shape)
print("Extracted:\n", extracted)

values = pd.to_numeric(extracted.iloc[:, 0], errors='coerce')
units = extracted.iloc[:, 1]

print("\nValues:", values.tolist())
print("Units:", units.tolist())

# Try assignment
df["Memory"] = values
print("\nAfter assignment:", df["Memory"].tolist())

# Try unit conversion
unit_map = {"MB": 1/1024, "GB": 1, "TB": 1024}
for unit_name, multiplier in unit_map.items():
    mask = units == unit_name
    print(f"\nMask for {unit_name}:", mask.tolist())
    df.loc[mask, "Memory"] = df.loc[mask, "Memory"] * multiplier

print("\nFinal:", df["Memory"].tolist())
