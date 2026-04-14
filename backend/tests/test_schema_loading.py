"""Test schema loading functionality."""
from app.core.config import load_schema, get_default_schema, list_available_schemas

output = []
output.append("SCHEMA LOADING TEST")
output.append("=" * 60)

# List available schemas
schemas = list_available_schemas()
output.append(f"\nAvailable schemas: {schemas}")

# Load default schema
schema = get_default_schema()
output.append(f"\nDefault schema version: {schema.get('version', 'N/A')}")
output.append(f"Description: {schema.get('description', 'N/A')}")

# Show fields
if "schema" in schema and "fields" in schema["schema"]:
    fields = schema["schema"]["fields"]
    output.append(f"\nDefined fields: {len(fields)}")
    
    for field_name, field_def in fields.items():
        aliases = field_def.get("aliases", [])
        field_type = field_def.get("type", "unknown")
        unit = field_def.get("unit", "N/A")
        pattern = field_def.get("pattern", "N/A")
        output.append(f"\n  {field_name}:")
        output.append(f"    Type: {field_type}")
        output.append(f"    Unit: {unit}")
        output.append(f"    Pattern: {pattern}")
        output.append(f"    Aliases ({len(aliases)}): {', '.join(aliases[:3])}{'...' if len(aliases) > 3 else ''}")

output.append("\n" + "=" * 60)
output.append("TEST PASSED!")

result = "\n".join(output)
print(result)

with open("schema_loading_test_output.txt", "w") as f:
    f.write(result)

