import re
from typing import Dict

import pandas as pd


EMAIL_RE = re.compile(r"^[\w.+-]+@[\w-]+\.[a-z]{2,}$", re.IGNORECASE)
PHONE_RE = re.compile(r"^\+?[\d\s\-().]{7,15}$")
URL_RE = re.compile(r"^https?://", re.IGNORECASE)
CURRENCY_RE = re.compile(r"^[$£€₹¥]")
PERCENT_STR_RE = re.compile(r"%\s*$")


def _match_ratio(series: pd.Series, pattern: re.Pattern) -> float:
    if series.empty:
        return 0.0
    matches = series.astype(str).str.strip().apply(lambda v: bool(pattern.match(v))).sum()
    return float(matches) / float(len(series))


def infer_semantic_types(df: pd.DataFrame) -> Dict[str, str]:
    """Return a dict mapping each column to a semantic type."""
    semantic_types: Dict[str, str] = {}
    row_count = max(int(df.shape[0]), 1)

    for col in df.columns:
        col_name = str(col)
        series = df[col]
        non_null = series.dropna()
        lowered_name = col_name.lower()

        if non_null.empty:
            semantic_types[col_name] = "UNKNOWN"
            continue

        is_numeric = pd.api.types.is_numeric_dtype(series)
        is_datetime = pd.api.types.is_datetime64_any_dtype(series)
        is_string_like = pd.api.types.is_object_dtype(series) or pd.api.types.is_string_dtype(series)

        if is_datetime:
            semantic_types[col_name] = "DATE_STRING"
            continue

        if is_string_like:
            string_values = non_null.astype(str).str.strip()

            if _match_ratio(string_values, EMAIL_RE) > 0.7:
                semantic_types[col_name] = "EMAIL"
                continue

            try:
                parsed_dates = pd.to_datetime(string_values, errors="coerce", format="mixed")
            except TypeError:
                parsed_dates = pd.to_datetime(string_values, errors="coerce")
            if float(parsed_dates.notna().sum()) / float(len(string_values)) > 0.7:
                semantic_types[col_name] = "DATE_STRING"
                continue

            if _match_ratio(string_values, PHONE_RE) > 0.7:
                semantic_types[col_name] = "PHONE_NUMBER"
                continue

            if _match_ratio(string_values, URL_RE) > 0.7:
                semantic_types[col_name] = "URL"
                continue

            if _match_ratio(string_values, PERCENT_STR_RE) > 0.7:
                semantic_types[col_name] = "PERCENTAGE"
                continue

            if _match_ratio(string_values, CURRENCY_RE) > 0.6:
                semantic_types[col_name] = "CURRENCY"
                continue

            unique_count = int(string_values.nunique(dropna=True))
            unique_ratio = float(unique_count) / float(row_count)
            avg_word_count = float(string_values.str.split().str.len().mean() or 0.0)

            if avg_word_count >= 5.0 or (unique_ratio > 0.50 and avg_word_count >= 3.0):
                semantic_types[col_name] = "FREE_TEXT"
                continue

            if unique_count < 20 and unique_ratio < 0.10:
                semantic_types[col_name] = "CATEGORICAL_LOW"
                continue

            if unique_count >= 20 and unique_ratio < 0.50:
                semantic_types[col_name] = "CATEGORICAL_HIGH"
                continue

            semantic_types[col_name] = "UNKNOWN"
            continue

        if is_numeric:
            numeric_values = pd.to_numeric(non_null, errors="coerce").dropna()
            if numeric_values.empty:
                semantic_types[col_name] = "UNKNOWN"
                continue

            if bool(numeric_values.between(0, 1).all()):
                semantic_types[col_name] = "PERCENTAGE"
                continue

            if "lat" in lowered_name and bool(numeric_values.between(-90, 90).all()):
                semantic_types[col_name] = "LATITUDE"
                continue

            if ("lon" in lowered_name or "lng" in lowered_name) and bool(numeric_values.between(-180, 180).all()):
                semantic_types[col_name] = "LONGITUDE"
                continue

            if any(token in lowered_name for token in ["price", "cost", "salary", "revenue", "fee"]):
                semantic_types[col_name] = "CURRENCY"
                continue

            if pd.api.types.is_integer_dtype(series) and int(numeric_values.nunique(dropna=True)) == len(numeric_values):
                semantic_types[col_name] = "NUMERIC_ID"
                continue

            semantic_types[col_name] = "NUMERIC_CONTINUOUS"
            continue

        semantic_types[col_name] = "UNKNOWN"

    return semantic_types
