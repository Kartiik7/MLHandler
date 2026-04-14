"""Type fixing utilities for MLHandler.

Responsibilities:
- Convert numeric-like strings to numeric types
- Convert date-like strings to datetime
- Handle invalid conversions by setting NaN
- Reapply missing value rules after conversion
- Extract numeric values from unit strings (e.g., "8GB" → 8, "1.83kg" → 1.83)

This module does not perform categorical encoding.
"""
from typing import Tuple, Dict, Any, Optional
import pandas as pd
import numpy as np
import re

try:
	from app.services.column_rules import COLUMN_RULES
except Exception:
	COLUMN_RULES = {}


def _extract_numeric_with_unit(value: str) -> Optional[float]:
	"""Extract numeric value from strings with units like '8GB', '1.83kg', '512GB SSD'.
	
	Returns the numeric part as float, or None if no match.
	Examples:
		'8GB' → 8.0
		'1.83kg' → 1.83
		'512GB SSD' → 512.0
	"""
	if not isinstance(value, str):
		return None
	
	# Pattern: optional whitespace, number (int or float), optional whitespace, unit letters
	# Captures just the numeric part
	match = re.match(r'^\s*([+-]?(?:\d+\.?\d*|\.\d+))\s*[a-zA-Z]+', value.strip())
	if match:
		try:
			return float(match.group(1))
		except (ValueError, AttributeError):
			return None
	return None


def clean_column_by_rule(df: pd.DataFrame, column_name: str, rule: Dict[str, Any]) -> Tuple[pd.DataFrame, int]:
	"""Clean a column by applying a rule from COLUMN_RULES.
	
	Args:
		df: Input DataFrame
		column_name: Name of the column to clean
		rule: Rule dictionary with 'regex', 'type', 'unit', 'allow_unitless'
		
	Returns:
		Tuple of (modified DataFrame, count of successful extractions)
	"""
	if column_name not in df.columns:
		return df, 0
	
	working = df.copy()
	pattern = re.compile(rule["regex"])
	converted_count = 0
	
	def extract(value):
		nonlocal converted_count
		if pd.isna(value):
			return None
		
		match = pattern.search(str(value))
		if match:
			try:
				converted_count += 1
				return float(match.group(1))
			except (ValueError, IndexError):
				return None
		return None
	
	# Apply extraction
	working[column_name] = working[column_name].apply(extract)
	
	# Fill NaNs with mean if we have any valid values
	if working[column_name].notna().sum() > 0:
		mean_val = working[column_name].mean()
		working[column_name] = working[column_name].fillna(mean_val)
	
	return working, converted_count


def apply_schema_based_type_fixes(df: pd.DataFrame, schema_fields: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
	"""Apply type transformations based on schema field definitions.
	
	This is the new schema-driven approach that replaces hardcoded column logic.
	Uses pattern matching from schema to extract and convert values.
	
	Args:
		df: Input DataFrame
		schema_fields: Dictionary of field definitions from schema
		
	Returns:
		Tuple of (transformed DataFrame, stats dict)
	"""
	working = df # No internal copy here, rely on caller or only copy if needed at top level
	stats = {"schema_based_conversions": {}}
	
	for canonical_col, field_def in schema_fields.items():
		if canonical_col not in working.columns:
			continue
		
		field_type = field_def.get("type")
		pattern = field_def.get("pattern")
		unit = field_def.get("unit", "unknown")
		
		# Only process numeric fields
		if field_type != "numeric":
			continue
		
		converted_count = 0
		original_values = working[canonical_col].copy()
		
		# Apply pattern extraction if pattern is defined
		if pattern:
			try:
				# Extract numeric values using regex pattern
				extracted = working[canonical_col].astype(str).str.extract(pattern, expand=False)
				
				# If pattern has multiple groups, take the first one
				if isinstance(extracted, pd.DataFrame):
					extracted = extracted.iloc[:, 0]
				
				# Convert to numeric
				working[canonical_col] = pd.to_numeric(extracted, errors='coerce')
				
				# Count successful conversions
				converted_count = working[canonical_col].notna().sum()
				
			except Exception as e:
				# If pattern fails, fall back to simple numeric conversion
				working[canonical_col] = pd.to_numeric(working[canonical_col], errors='coerce')
				converted_count = working[canonical_col].notna().sum()
		else:
			# No pattern, just convert to numeric
			working[canonical_col] = pd.to_numeric(working[canonical_col], errors='coerce')
			converted_count = working[canonical_col].notna().sum()
		
		# Fill NaNs with mean if we have valid values
		if working[canonical_col].notna().sum() > 0:
			mean_val = working[canonical_col].mean()
			working[canonical_col] = working[canonical_col].fillna(mean_val)
		
		# Record stats
		stats["schema_based_conversions"][canonical_col] = {
			"converted_count": converted_count,
			"unit": unit,
			"pattern_used": pattern if pattern else "none"
		}
	
	# Apply unit standardization if enabled
	if any(field.get("unit_standardization") for field in schema_fields.values()):
		working, unit_stats = convert_units(working, schema_fields)
		stats.update(unit_stats)
	
	return working, stats


def convert_units(df: pd.DataFrame, schema_fields: Dict[str, Any]) -> Tuple[pd.DataFrame, Dict[str, Any]]:
	"""Convert values to standard units based on schema configuration.
	
	Handles unit standardization for fields like Memory (MB/GB/TB → GB).
	Uses unit_pattern from schema to extract value and unit, then converts
	to the standard unit specified in the schema.
	
	Args:
		df: Input DataFrame
		schema_fields: Dictionary of field definitions from schema
		
	Returns:
		Tuple of (converted DataFrame, stats dict)
	"""
	working = df # Internal helper, modification is on existing working copy from caller
	stats = {"unit_conversions": {}}
	
	# Default unit conversion maps
	UNIT_MAPS = {
		"GB": {"MB": 1/1024, "GB": 1, "TB": 1024},
		"kg": {"g": 1/1000, "kg": 1, "lb": 0.453592},
		"inches": {"cm": 0.393701, "inches": 1, "mm": 0.0393701}
	}
	
	for col, field_def in schema_fields.items():
		if col not in working.columns:
			continue
		
		# Only process fields with unit_standardization enabled
		if not field_def.get("unit_standardization"):
			continue
		
		standard_unit = field_def.get("unit")
		unit_pattern = field_def.get("unit_pattern", r"(\d+\.?\d*)\s*(MB|GB|TB)")
		
		if not standard_unit or standard_unit not in UNIT_MAPS:
			continue
		
		unit_map = UNIT_MAPS[standard_unit]
		converted_count = 0
		
		try:
			# Extract value and unit using regex
			extracted = working[col].astype(str).str.extract(unit_pattern, expand=True)
			
			if extracted.shape[1] >= 2:
				values = pd.to_numeric(extracted.iloc[:, 0], errors='coerce')
				units = extracted.iloc[:, 1]
				
				# Start with the extracted values
				working[col] = values
				
				# Apply unit conversions using loc for proper indexing
				for unit_name, multiplier in unit_map.items():
					mask = units == unit_name
					working.loc[mask, col] = working.loc[mask, col] * multiplier
					converted_count += mask.sum()
				
				# Fill NaNs with mean
				if working[col].notna().sum() > 0:
					mean_val = working[col].mean()
					working[col] = working[col].fillna(mean_val)
				
				stats["unit_conversions"][col] = {
					"converted_count": int(converted_count),
					"standard_unit": standard_unit,
					"pattern_used": unit_pattern
				}
		
		except Exception as e:
			# If conversion fails, skip this column
			stats["unit_conversions"][col] = {
				"converted_count": 0,
				"error": str(e)
			}
	
	return working, stats


def fix_columns_by_rules(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, Any]]:
	"""Apply COLUMN_RULES to all matching columns in the DataFrame.
	
	DEPRECATED: Use apply_schema_based_type_fixes() instead.
	This function is kept for backward compatibility.
	
	Args:
		df: Input DataFrame
		
	Returns:
		Tuple of (cleaned DataFrame, stats dict with conversion counts)
	"""
	working = df # Internal helper
	stats = {"rule_based_conversions": {}}
	
	for col_name, rule in COLUMN_RULES.items():
		if col_name in working.columns:
			working, converted_count = clean_column_by_rule(working, col_name, rule)
			stats["rule_based_conversions"][col_name] = {
				"converted_count": converted_count,
				"rule_applied": True,
				"unit": rule.get("unit", "unknown")
			}
	
	return working, stats




def fix_types(df: pd.DataFrame, config: Optional[Dict[str, Any]] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
	"""Attempt to coerce column types for a DataFrame.

	Returns a tuple (fixed_df, stats) where stats contains per-column
	information about conversions performed.
	"""
	if not isinstance(df, pd.DataFrame):
		raise TypeError("df must be a pandas DataFrame")

	working = df.copy()

	# Determine whether to perform type conversions
	convert_types = True
	if isinstance(config, dict) and "convert_types" in config:
		convert_types = bool(config.get("convert_types"))

	if not convert_types:
		# Return early with empty conversions stats
		stats: Dict[str, Any] = {"conversions": {}, "before_dtypes": {}, "after_dtypes": {}}
		for col in working.columns:
			stats["before_dtypes"][str(col)] = str(working[col].dtype)
			stats["conversions"][str(col)] = {"action": "skipped", "reason": "convert_types_disabled"}
			stats["after_dtypes"][str(col)] = str(working[col].dtype)
		return working, stats
	stats: Dict[str, Any] = {"conversions": {}, "before_dtypes": {}, "after_dtypes": {}}

	# NEW: Use schema-based type fixing if schema is provided
	if config and "schema" in config and "fields" in config["schema"]:
		schema_fields = config["schema"]["fields"]
		working, schema_stats = apply_schema_based_type_fixes(working, schema_fields)
		stats.update(schema_stats)
	else:
		# FALLBACK: Use old rule-based approach for backward compatibility
		working, rule_stats = fix_columns_by_rules(working)
		stats.update(rule_stats)

	for col in working.columns:
		ser = working[col]
		stats["before_dtypes"][str(col)] = str(ser.dtype)

		# Try numeric conversion where appropriate: if majority of non-null values look numeric
		non_null = ser.dropna()
		converted_any = False

		if non_null.empty:
			stats["conversions"][str(col)] = {"action": "none", "reason": "empty"}
			stats["after_dtypes"][str(col)] = str(working[col].dtype)
			continue

		# If currently numeric, skip
		if pd.api.types.is_numeric_dtype(ser.dtype):
			stats["conversions"][str(col)] = {"action": "none", "reason": "already_numeric"}
			stats["after_dtypes"][str(col)] = str(working[col].dtype)
			continue

		# Try to extract numeric values from unit strings (e.g., "8GB" → 8)
		unit_extracted = non_null.apply(_extract_numeric_with_unit)
		unit_extracted_count = unit_extracted.notna().sum()
		
		# If many values have extractable units, apply extraction to whole column
		if unit_extracted_count >= max(1, int(0.3 * len(non_null))):
			working[col] = working[col].apply(_extract_numeric_with_unit)
			converted_any = True
			stats["conversions"][str(col)] = {"action": "unit_extraction", "converted_count": int(working[col].notna().sum())}

		# Heuristic: try to coerce to numeric (with comma removal)
		if not converted_any:
			coerced_num = pd.to_numeric(non_null.astype(str).str.replace(r"[, ]+", "", regex=True), errors="coerce")
			num_non_na = coerced_num.notna().sum()
			# If at least half of non-null values coerce to numeric, convert whole column
			if num_non_na >= max(1, int(0.5 * len(non_null))):
				# apply to full column, preserving NaN
				working[col] = pd.to_numeric(working[col].astype(str).str.replace(r"[, ]+", "", regex=True), errors="coerce")
				converted_any = True
				stats["conversions"][str(col)] = {"action": "to_numeric", "converted_count": int(working[col].notna().sum())}

		# If not converted to numeric, try datetime coercion
		if not converted_any:
			try:
				coerced_dt = pd.to_datetime(non_null.astype(str), errors="coerce", format="mixed")
			except TypeError:
				coerced_dt = pd.to_datetime(non_null.astype(str), errors="coerce")
			dt_non_na = coerced_dt.notna().sum()
			# If a reasonable fraction converts, apply
			if dt_non_na >= max(1, int(0.5 * len(non_null))):
				try:
					working[col] = pd.to_datetime(working[col].astype(str), errors="coerce", format="mixed")
				except TypeError:
					working[col] = pd.to_datetime(working[col].astype(str), errors="coerce")
				converted_any = True
				stats["conversions"][str(col)] = {"action": "to_datetime", "converted_count": int(working[col].notna().sum())}

		if not converted_any:
			stats["conversions"][str(col)] = {"action": "none", "reason": "no_conversion_rule_matched"}

		# After attempts, ensure invalid conversions are NaN (pandas coercion already does that)
		stats["after_dtypes"][str(col)] = str(working[col].dtype)

	# Optimize missing normalization using vectorized replace
	missing_tokens = ["", "NA", "N/A", "NULL", "null", "None", "none"]
	try:
		# Use a single dictionary replace for speed
		replace_dict = {t: np.nan for t in missing_tokens}
		working = working.replace(replace_dict)
	except Exception:
		for t in missing_tokens:
			working = working.replace(t, np.nan)

	return working, stats