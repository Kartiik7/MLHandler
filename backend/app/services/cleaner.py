"""Deterministic data cleaning utilities for MLHandler.

Rules implemented:
- Trim whitespace in string columns
- Normalize missing values (NaN, empty, 'NA', 'null', 'None')
- Numeric columns: fill missing with median
- Categorical columns: fill missing with mode, or 'Unknown' if no mode
- Remove full-row duplicates (keep first)
- Do not drop columns unless all values are missing

Returns cleaned DataFrame and a stats dictionary describing actions taken.
"""
from typing import Tuple, Dict, Any, Optional
import pandas as pd
import numpy as np
try:
	from app.core.config import get_cleaning_rules
except Exception:
	# Fallback if config is not available in some contexts
	def get_cleaning_rules(overrides=None):
		return {
			"fill_missing_numeric": "median",
			"fill_missing_categorical": "mode",
			"drop_duplicates": True,
		}


def clean_dataframe(df: pd.DataFrame, overrides: Optional[Dict[str, Any]] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
	"""Clean `df` deterministically and return (cleaned_df, stats).

	The function does not perform encoding, scaling, or column drops
	except when a column is entirely missing (all values NA), in which
	case the column name is reported and dropped.
	"""
	if not isinstance(df, pd.DataFrame):
		raise TypeError("df must be a pandas DataFrame")

	before_rows = int(df.shape[0])
	before_cols = int(df.shape[1])

	working = df.copy()

	# Capture preview samples before cleaning (first 5 values per column)
	preview_samples: Dict[str, Dict[str, list]] = {}
	for col in df.columns:
		preview_samples[str(col)] = {
			"before": df[col].head(5).tolist()
		}

	stats: Dict[str, Any] = {
		"before_rows": before_rows,
		"before_columns": before_cols,
		"dropped_columns": [],
		"duplicates_removed": 0,
		"imputed": {},
		"missing_before": {},
		"missing_after": {},
		"preview_samples": preview_samples,
	}

	# Get effective cleaning rules (may come from defaults, env, or overrides)
	rules = get_cleaning_rules(overrides)

	# Normalize obvious missing-value strings to actual NA
	missing_tokens = ["", "NA", "N/A", "NULL", "null", "None", "none"]
	trim_strings = bool(rules.get("trim_strings", True))

	# Apply strip for object columns first to handle '  NA ' cases when trimming enabled
	obj_cols = working.select_dtypes(include=[object]).columns.tolist()
	if trim_strings:
		for col in obj_cols:
			ser = working[col]
			def _strip_val(v):
				if pd.isna(v):
					return v
				try:
					return v.strip()
				except Exception:
					return v

			working[col] = ser.map(_strip_val)

	# Replace known textual missing tokens with np.nan. If trimming is enabled
	# allow surrounding whitespace in the pattern; otherwise match exact token.
	if trim_strings:
		token_patterns = [r"^\\s*" + t + r"\\s*$" for t in missing_tokens if t]
	else:
		token_patterns = [r"^" + t + r"$" for t in missing_tokens if t]

	regex_pattern = "|".join(token_patterns)
	try:
		working = working.replace(to_replace=regex_pattern, value=np.nan, regex=True)
	except Exception:
		for t in missing_tokens:
			working = working.replace(t, np.nan)

	# Record missing before per column
	for col in working.columns:
		stats["missing_before"][str(col)] = int(working[col].isna().sum())

	# Drop columns that are entirely missing if configured
	drop_empty_columns = bool(rules.get("drop_empty_columns", True))
	for col in list(working.columns):
		if working[col].isna().all():
			if drop_empty_columns:
				stats["dropped_columns"].append(str(col))
				working.drop(columns=[col], inplace=True)
			else:
				# keep column but report it
				stats["dropped_columns"] = stats.get("dropped_columns", [])

	# Trim whitespace in any remaining object/string columns if enabled
	if trim_strings:
		for col in working.select_dtypes(include=[object]).columns:
			working[col] = working[col].map(lambda v: v.strip() if isinstance(v, str) else v)

	# Duplicate removal may be controlled by config (overrides allowed)
	rules = get_cleaning_rules(overrides)
	drop_duplicates = bool(rules.get("drop_duplicates", True))
	if drop_duplicates:
		before_dup_rows = int(working.shape[0])
		working = working.drop_duplicates(keep="first")
		after_dup_rows = int(working.shape[0])
		stats["duplicates_removed"] = before_dup_rows - after_dup_rows
	else:
		stats["duplicates_removed"] = 0

	# Impute numeric columns based on config strategy
	num_cols = working.select_dtypes(include=[np.number]).columns.tolist()
	num_strategy = str(rules.get("fill_missing_numeric", "median")).lower()
	for col in num_cols:
		missing_before = int(working[col].isna().sum())
		if missing_before > 0:
			if num_strategy == "median":
				fill_val = working[col].median()
			elif num_strategy == "mean":
				fill_val = working[col].mean()
			elif num_strategy == "zero":
				fill_val = 0
			else:
				# unsupported or 'none' -> skip imputation
				fill_val = None

			if fill_val is not None and not pd.isna(fill_val):
				working[col] = working[col].fillna(fill_val)
				stats["imputed"][str(col)] = int(missing_before)

	# Impute categorical columns based on config strategy
	cat_strategy = str(rules.get("fill_missing_categorical", "mode")).lower()
	cat_cols = [c for c in working.columns if c not in num_cols and not pd.api.types.is_datetime64_any_dtype(working[c])]
	for col in cat_cols:
		missing_before = int(working[col].isna().sum())
		if missing_before > 0:
			fill_val = None
			if cat_strategy == "mode":
				try:
					mode_vals = working[col].mode(dropna=True)
					if len(mode_vals) > 0 and not pd.isna(mode_vals.iloc[0]):
						fill_val = mode_vals.iloc[0]
				except Exception:
					fill_val = None
				if fill_val is None:
					fill_val = "Unknown"
			elif cat_strategy == "unknown":
				fill_val = "Unknown"
			elif cat_strategy == "none":
				fill_val = None
			else:
				# unsupported strategy -> fall back to mode/Unknown
				try:
					mode_vals = working[col].mode(dropna=True)
					if len(mode_vals) > 0 and not pd.isna(mode_vals.iloc[0]):
						fill_val = mode_vals.iloc[0]
				except Exception:
					fill_val = "Unknown"

			if fill_val is not None:
				working[col] = working[col].fillna(fill_val)
				stats["imputed"][str(col)] = int(missing_before)

	# Record missing after per column
	for col in working.columns:
		stats["missing_after"][str(col)] = int(working[col].isna().sum())

	# Capture preview samples after cleaning (first 5 values per column)
	for col in working.columns:
		col_str = str(col)
		if col_str in preview_samples:
			preview_samples[col_str]["after"] = working[col].head(5).tolist()
		else:
			# Column might be new (unlikely in cleaning but handle it)
			preview_samples[col_str] = {
				"before": [],
				"after": working[col].head(5).tolist()
			}

	stats["after_rows"] = int(working.shape[0])
	stats["after_columns"] = int(working.shape[1])

	return working, stats

