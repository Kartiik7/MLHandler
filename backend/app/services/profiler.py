"""DataFrame profiling utilities for MLHandler.

Read-only analysis that returns a structured summary per column.
"""
from typing import Any, Dict, Optional
import pandas as pd


def profile_dataframe(
	df: pd.DataFrame,
	categorical_threshold: int = 50,
	semantic_types: Optional[Dict[str, str]] = None,
) -> Dict[str, Any]:
	"""Produce a lightweight profile of a pandas DataFrame.

	The function is read-only and does not modify `df`.

	Returns a dict with top-level keys: `n_rows`, `n_columns`, and
	`columns` where `columns` maps column names to their summaries.
	"""
	if not isinstance(df, pd.DataFrame):
		raise TypeError("`df` must be a pandas DataFrame")

	summary: Dict[str, Any] = {
		"n_rows": int(df.shape[0]),
		"n_columns": int(df.shape[1]),
		"columns": {},
	}

	for col in df.columns:
		ser = df[col]
		dtype = str(ser.dtype)
		missing_count = int(ser.isna().sum())
		missing_percent = float(missing_count) / max(1, int(df.shape[0]))
		# Efficiency: if dataset is very large, limit nunique or use sampled estimation
		if df.shape[0] > 50000:
			# Estimate distinct count from a sample (approximate but much faster)
			sample_size = 10000
			sample_nunique = int(ser.sample(n=sample_size, random_state=42).nunique(dropna=True))
			distinct_count = int(min(df.shape[0], sample_nunique * (df.shape[0] / sample_size)))
			is_estimated_cardinality = True
		else:
			distinct_count = int(ser.nunique(dropna=True))
			is_estimated_cardinality = False

		is_numeric = pd.api.types.is_numeric_dtype(ser.dtype)
		is_datetime = pd.api.types.is_datetime64_any_dtype(ser.dtype)
		# Treat as categorical if non-numeric or low cardinality
		is_categorical = (not is_numeric and not is_datetime) or (distinct_count <= categorical_threshold)

		col_summary = {
			"dtype": dtype,
			"missing_count": missing_count,
			"missing_percent": round(missing_percent, 6),
			"distinct_count": distinct_count,
			"distinct_count_estimated": is_estimated_cardinality,
			"is_numeric": bool(is_numeric),
			"is_datetime": bool(is_datetime),
			"is_categorical": bool(is_categorical),
			"semantic_type": (semantic_types or {}).get(str(col), "UNKNOWN"),
		}

		# Add simple numeric stats when applicable
		if is_numeric:
			# use .describe to avoid expensive computations for large series
			desc = ser.describe()
			try:
				numeric_ser = pd.to_numeric(ser, errors="coerce").dropna()
				q1 = float(numeric_ser.quantile(0.25)) if not numeric_ser.empty else None
				median = float(numeric_ser.quantile(0.50)) if not numeric_ser.empty else None
				q3 = float(numeric_ser.quantile(0.75)) if not numeric_ser.empty else None
				iqr = (q3 - q1) if q1 is not None and q3 is not None else None
				whisker_low = (q1 - 1.5 * iqr) if iqr is not None else None
				whisker_high = (q3 + 1.5 * iqr) if iqr is not None else None
				outlier_count = 0
				if whisker_low is not None and whisker_high is not None:
					outlier_count = int(((numeric_ser < whisker_low) | (numeric_ser > whisker_high)).sum())

				col_summary.update(
					{
						"min": None if pd.isna(desc.get("min")) else float(desc.get("min")),
						"max": None if pd.isna(desc.get("max")) else float(desc.get("max")),
						"mean": None if pd.isna(desc.get("mean")) else float(desc.get("mean")),
						"std": None if pd.isna(desc.get("std")) else float(desc.get("std")),
						"q1": q1,
						"median": median,
						"q3": q3,
						"iqr": iqr,
						"whisker_low": whisker_low,
						"whisker_high": whisker_high,
						"outlier_count": outlier_count,
					}
				)
			except Exception:
				pass

		# include small sample of distinct values (up to 5)
		try:
			sample_vals = ser.dropna().unique()[:5].tolist()
			# convert numpy types to python native types where possible
			sample_vals = [v.item() if hasattr(v, "item") else v for v in sample_vals]
			col_summary["sample_values"] = sample_vals
		except Exception:
			col_summary["sample_values"] = []

		summary["columns"][str(col)] = col_summary

	return summary

