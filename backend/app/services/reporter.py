"""Generate a processing summary report for MLHandler MVP.

Produces a JSON-serializable dictionary summarizing cleaning and
type-fixing steps taken during processing.
"""
from typing import Dict, Any, Optional


def generate_report(clean_stats: Dict[str, Any], type_stats: Optional[Dict[str, Any]] = None, rename_map: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
    """Build a clean, JSON-serializable report from component stats.

    Args:
        clean_stats: Stats returned by `cleaner.clean_dataframe`.
        type_stats: Stats returned by `type_fixer.fix_types` (optional).
        rename_map: Column rename mapping returned by validator (optional).

    Returns:
        dict: JSON-serializable report.
    """
    report: Dict[str, Any] = {}

    # Rows before / after
    report["rows_before"] = int(clean_stats.get("before_rows", 0))
    report["rows_after"] = int(clean_stats.get("after_rows", report["rows_before"]))

    # Duplicates removed
    report["duplicates_removed"] = int(clean_stats.get("duplicates_removed", 0))

    # Columns dropped
    report["columns_dropped"] = list(clean_stats.get("dropped_columns", []))

    # Missing values filled: sum of imputed counts
    imputed = clean_stats.get("imputed", {}) or {}
    total_imputed = 0
    imputed_per_column = {}
    for col, count in imputed.items():
        try:
            c = int(count)
        except Exception:
            c = 0
        imputed_per_column[str(col)] = c
        total_imputed += c

    report["missing_filled_total"] = total_imputed
    report["missing_filled_per_column"] = imputed_per_column

    # Type conversions
    type_conversions = {}
    if type_stats and isinstance(type_stats, dict):
        convs = type_stats.get("conversions", {}) or {}
        for col, info in convs.items():
            # Ensure serializable
            type_conversions[str(col)] = info
    report["type_conversions"] = type_conversions

    # Columns renamed by validator (if any)
    report["columns_renamed"] = rename_map or {}

    # Include before/after column counts if present
    if "before_columns" in clean_stats:
        report["columns_before"] = int(clean_stats.get("before_columns"))
    if "after_columns" in clean_stats:
        report["columns_after"] = int(clean_stats.get("after_columns"))

    # Optionally include the full clean_stats and type_stats under debug key (kept minimal)
    report["_raw_clean_stats"] = {k: clean_stats.get(k) for k in ("missing_before", "missing_after") if k in clean_stats}

    return report
