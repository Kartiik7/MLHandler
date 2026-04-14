"""Dataset validation utilities for MLHandler.

Responsibilities:
- Validate dataframe structure and integrity
- Schema validation against configuration
"""

from typing import Tuple, Dict, List, Optional, Any
import pandas as pd
import numpy as np


class DatasetValidationError(ValueError):
	"""Raised when a dataset fails validation checks."""


def _auto_rename_duplicates(columns: List[str]) -> Tuple[List[str], Dict[str, str]]:
	"""Return a new list of column names where duplicates are suffixed to be unique.

	Also return a mapping of original_name -> new_name for those that changed.
	"""
	seen: Dict[str, int] = {}
	new_cols: List[str] = []
	mapping: Dict[str, str] = {}
	for col in columns:
		base = col if col is not None else ""
		count = seen.get(base, 0)
		if count == 0:
			new_name = base
		else:
			new_name = f"{base}_{count}"
		seen[base] = count + 1
		new_cols.append(new_name)
		if new_name != base:
			mapping[base] = new_name
	return new_cols, mapping


def validate_dataframe(df: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
	"""Validate and normalize a DataFrame according to MVP rules.

	Returns a tuple `(validated_df, rename_map)` where `rename_map` maps
	original column names to new names for any renamed columns.

	Raises:
		DatasetValidationError: if validation fails.
	"""
	if not isinstance(df, pd.DataFrame):
		raise DatasetValidationError("Input must be a pandas DataFrame")

	# Work on a shallow copy to avoid modifying caller's object
	working = df.copy()

	# Auto-rename duplicate columns
	orig_cols = [str(c) if c is not None else "" for c in list(working.columns)]
	new_cols, rename_map = _auto_rename_duplicates(orig_cols)
	if new_cols != orig_cols:
		working.columns = new_cols

	# Check for empty column names (after renaming)
	for i, col in enumerate(working.columns):
		if (col or "").strip() == "":
			raise DatasetValidationError(f"Empty column name at position {i}")

	# Check for completely empty columns
	empty_columns = [str(col) for col in working.columns if working[col].isna().all()]
	if empty_columns:
		raise DatasetValidationError(f"Completely empty columns found: {empty_columns}")

	# Check that at least one row remains
	if int(working.shape[0]) == 0:
		raise DatasetValidationError("Dataset contains no rows after processing")

	return working, rename_map


def validate_schema(df: pd.DataFrame, schema_config: Optional[Dict[str, Any]] = None) -> List[Dict[str, str]]:
	"""Validate DataFrame against a schema configuration.
	
	Args:
		df: DataFrame to validate
		schema_config: Dictionary with schema rules, e.g.:
			{
				"fields": {
					"Feature_A": {"required": True, "type": "numeric"},
					"Feature_B": {"required": True, "type": "numeric"},
					"Feature_C": {"required": False, "type": "string"}
				}
			}
	
	Returns:
		List of validation issues, each a dict with "column" and "issue" keys.
		Empty list means validation passed.
	
	Example:
		[
			{"column": "Feature_A", "issue": "missing values found"},
			{"column": "Feature_C", "issue": "expected numeric type, found object"}
		]
	"""
	issues: List[Dict[str, str]] = []
	
	if not schema_config or "fields" not in schema_config:
		# No schema to validate against
		return issues
	
	fields = schema_config.get("fields", {})
	
	for field_name, rules in fields.items():
		if field_name not in df.columns:
			# Column doesn't exist in DataFrame
			issues.append({
				"column": field_name,
				"issue": "column not found in dataset"
			})
			continue
		
		col = df[field_name]
		
		# Check if required field has missing values
		if rules.get("required", False):
			missing_count = int(col.isna().sum())
			if missing_count > 0:
				issues.append({
					"column": field_name,
					"issue": f"missing values found (required field has {missing_count} nulls)"
				})
		
		# Check type constraints
		expected_type = rules.get("type")
		if expected_type:
			actual_dtype = str(col.dtype)
			
			if expected_type == "numeric":
				if not pd.api.types.is_numeric_dtype(col):
					issues.append({
						"column": field_name,
						"issue": f"expected numeric type, found {actual_dtype}"
					})
			elif expected_type == "string":
				if not pd.api.types.is_string_dtype(col) and not pd.api.types.is_object_dtype(col):
					issues.append({
						"column": field_name,
						"issue": f"expected string type, found {actual_dtype}"
					})
			elif expected_type == "datetime":
				if not pd.api.types.is_datetime64_any_dtype(col):
					issues.append({
						"column": field_name,
						"issue": f"expected datetime type, found {actual_dtype}"
					})
			elif expected_type == "boolean":
				if not pd.api.types.is_bool_dtype(col):
					issues.append({
						"column": field_name,
						"issue": f"expected boolean type, found {actual_dtype}"
					})
		
		# Check min/max constraints for numeric fields
		if pd.api.types.is_numeric_dtype(col):
			if "min" in rules:
				min_val = rules["min"]
				actual_min = col.min()
				if not pd.isna(actual_min) and actual_min < min_val:
					issues.append({
						"column": field_name,
						"issue": f"value {actual_min} is below minimum {min_val}"
					})
			
			if "max" in rules:
				max_val = rules["max"]
				actual_max = col.max()
				if not pd.isna(actual_max) and actual_max > max_val:
					issues.append({
						"column": field_name,
						"issue": f"value {actual_max} exceeds maximum {max_val}"
					})
	
	return issues

