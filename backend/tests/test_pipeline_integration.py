"""Test pipeline integration with FieldMapper."""
import pandas as pd
import tempfile
import os
import sys
from app.services.pipeline import process_csv

# Redirect all output to file
output_file = open("pipeline_integration_test_results.txt", "w")
sys.stdout = output_file
sys.stderr = output_file

try:
    # Create test CSV with inconsistent column names
    test_data = {
        "ram": ["8GB", "16GB", "4GB", "32GB"],              # Lowercase alias
        "WEIGHT": ["1.83kg", "2.1kg", "1.5kg", "2.5kg"],    # Uppercase alias
        "Company": ["Dell", "HP", "Lenovo", "Apple"],        # Exact match
        "product_price": [500, 800, 600, 1200],             # Alias
    }

    df = pd.DataFrame(test_data)

    # Save to temporary CSV
    fd, input_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    df.to_csv(input_path, index=False)

    print("=" * 70)
    print("PIPELINE INTEGRATION TEST WITH FIELD MAPPER")
    print("=" * 70)

    print("\n📊 INPUT CSV:")
    print(df)
    print("\nInput columns:", list(df.columns))

    # Define schema with field mappings
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
                "aliases": ["company", "manufacturer", "brand"],
                "type": "categorical"
            },
            "Price": {
                "aliases": ["price", "cost", "product_price"],
                "type": "numeric",
                "unit": "USD"
            }
        }
    }

    # Create config with schema
    config = {
        "schema": schema,
        "fill_missing_numeric": "median",
        "fill_missing_categorical": "mode",
        "drop_duplicates": True,
        "convert_types": True
    }

    print("\n🔧 Running pipeline with schema-based field mapping...")

    # Run pipeline
    output_path, report = process_csv(input_path, config)
    
    # Read output CSV
    df_output = pd.read_csv(output_path)
    
    print("\n✅ PIPELINE COMPLETE!")
    print("\n📊 OUTPUT CSV:")
    print(df_output)
    print("\nOutput columns:", list(df_output.columns))
    print("\nOutput dtypes:")
    print(df_output.dtypes)
    
    # Show field mapping stats
    if "field_mapping" in report:
        fm_stats = report["field_mapping"]
        print("\n📋 FIELD MAPPING STATS:")
        print(f"  Total columns: {fm_stats.get('total_columns', 0)}")
        print(f"  Mapped: {fm_stats.get('mapped_count', 0)}")
        print(f"  Unmapped: {fm_stats.get('unmapped_count', 0)}")
        
        if fm_stats.get('mappings'):
            print("\n  Mappings:")
            for orig, info in fm_stats['mappings'].items():
                print(f"    '{orig}' → '{info['mapped_to']}' ({info['match_type']})")
        
        if fm_stats.get('unmapped'):
            print(f"\n  Unmapped columns: {fm_stats['unmapped']}")
    
    # Show type conversion stats
    if "type_stats" in report and "rule_based_conversions" in report["type_stats"]:
        print("\n🔄 TYPE CONVERSIONS:")
        for col, stats in report["type_stats"]["rule_based_conversions"].items():
            print(f"  {col}: {stats['converted_count']} values converted ({stats['unit']})")
    
    # Cleanup
    os.remove(input_path)
    os.remove(output_path)
    
    print("\n" + "=" * 70)
    print("✅ TEST PASSED - Pipeline integration successful!")
    print("=" * 70)
    
except Exception as e:
    print(f"\n❌ TEST FAILED: {e}")
    import traceback
    traceback.print_exc()
    
    # Cleanup on error
    if 'input_path' in locals() and os.path.exists(input_path):
        os.remove(input_path)

finally:
    output_file.close()
    sys.stdout = sys.__stdout__
    sys.stderr = sys.__stderr__
    print("✅ Test complete! Check pipeline_integration_test_results.txt for full output")

