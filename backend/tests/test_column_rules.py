"""Quick test to verify rule-based column cleaning works."""
import pandas as pd
from app.services.type_fixer import fix_columns_by_rules
import sys

# Create test data
test_data = {
    "Ram": ["8GB", "16GB", "4GB", "32GB"],
    "Weight": ["1.83kg", "2.1kg", "1.5kg", "2.5kg"],
    "Memory": ["512GB SSD", "256GB", "1TB HDD", "128GB"],
    "Price": ["$500", "$800", "$600", "$1200"]
}

df = pd.DataFrame(test_data)

# Redirect output to file
with open("test_output.txt", "w") as f:
    f.write("BEFORE:\n")
    f.write(str(df) + "\n")
    f.write("\nDtypes before:\n")
    f.write(str(df.dtypes) + "\n")

    # Apply rule-based cleaning
    df_cleaned, stats = fix_columns_by_rules(df)

    f.write("\n\nAFTER:\n")
    f.write(str(df_cleaned) + "\n")
    f.write("\nDtypes after:\n")
    f.write(str(df_cleaned.dtypes) + "\n")

    f.write("\n\nStats:\n")
    f.write(str(stats) + "\n")

print("✅ Test completed! Check test_output.txt for results")
