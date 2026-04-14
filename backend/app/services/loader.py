"""CSV loading utility for MLHandler.

Responsibilities:
- Load CSV into a pandas DataFrame
- Try multiple encodings (fallback)
- Reject empty files or files with no rows/columns

This module only handles loading/validation — no cleaning logic.
"""
from typing import List, Optional, Tuple, Dict, Any
import os
import pandas as pd


def load_csv(path: str, encodings: Optional[List[str]] = None) -> Tuple[pd.DataFrame, Dict[str, Any]]:
	"""Load a CSV file into a pandas DataFrame with encoding fallback.

	Args:
		path: Path to the CSV file.
		encodings: Optional list of encodings to try (in order). If not
			provided, callers should pass their defaults (e.g. from config).

	Raises:
		ValueError: for missing file, empty file, unreadable file, or invalid dataframe.

	Returns:
		Tuple of (DataFrame, metadata) where metadata includes `rows`, `columns`, and `encoding`.
	"""
	if not os.path.exists(path):
		raise ValueError(f"File not found: {path}")

	if os.path.getsize(path) == 0:
		raise ValueError("Uploaded file is empty")

	tried: List[str] = []
	encs = list(encodings) if encodings else ["utf-8", "latin-1"]
	last_exc: Optional[Exception] = None
	for enc in encs:
		tried.append(enc)
		try:
			df = pd.read_csv(path, encoding=enc, low_memory=False)
		except UnicodeDecodeError as exc:
			last_exc = exc
			continue
		except Exception as exc:
			last_exc = exc
			continue

		if df is None:
			raise ValueError(f"Failed to read CSV with encoding {enc}")

		# Basic structural checks
		rows, cols = df.shape
		if cols == 0:
			raise ValueError(f"CSV contains no columns: {path}")
		if rows == 0:
			raise ValueError(f"CSV contains no rows: {path}")

		# Normalize empty strings to NA and check for all-missing
		df = df.replace(r'^\s*$', pd.NA, regex=True)
		if df.isna().all().all():
			raise ValueError(f"CSV contains only missing values (all cells are null/empty): {path}")

		metadata: Dict[str, Any] = {"rows": rows, "columns": cols, "encoding": enc}
		return df, metadata

	# If we reach here, all encodings failed
	raise ValueError(f"Failed to read CSV; tried encodings: {tried}. Last error: {last_exc}")
