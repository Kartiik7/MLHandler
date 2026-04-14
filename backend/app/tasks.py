"""Celery tasks for MLHandler.

This module orchestrates the existing service pipeline with progress updates,
lineage tracking, semantic inference, and multi-format output export.
"""
from __future__ import annotations

import json
import os
import re
from typing import Any, Dict, Optional

from celery import current_task

from app.celery_app import celery_app
from app.core.config import DOWNLOADS_DIR
from app.core.logger import get_logger
from app.services.lineage import LineageTracker

logger = get_logger("app.tasks")

os.makedirs(DOWNLOADS_DIR, exist_ok=True)

_ILLEGAL_EXCEL_CHARS_RE = re.compile(r"[\x00-\x08\x0B-\x0C\x0E-\x1F]")
_EXCEL_MAX_CELL_LEN = 32767


def _update(percent: int, stage: str) -> None:
    """Push a PROGRESS state to the Celery result backend."""
    current_task.update_state(
        state="PROGRESS",
        meta={"percent": percent, "stage": stage, "status": "processing"},
    )


def _make_serializable(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _make_serializable(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_make_serializable(v) for v in obj]

    if hasattr(obj, "item"):
        try:
            obj = obj.item()
        except Exception:
            pass

    if hasattr(obj, "isoformat"):
        try:
            return obj.isoformat()
        except Exception:
            pass

    if isinstance(obj, float):
        if obj != obj or obj in (float("inf"), float("-inf")):
            return None
        return obj

    try:
        import numpy as np

        if isinstance(obj, (np.integer,)):
            return int(obj)
        if isinstance(obj, (np.floating,)):
            return float(obj)
        if isinstance(obj, np.ndarray):
            return obj.tolist()
    except Exception:
        pass
    if isinstance(obj, (str, int, bool)) or obj is None:
        return obj
    return str(obj)


def _sanitize_excel_cell(value: Any) -> Any:
    """Return a value that openpyxl can safely write to Excel."""
    if value is None:
        return None

    if hasattr(value, "item"):
        try:
            value = value.item()
        except Exception:
            pass

    # Excel cannot store timezone-aware datetime values.
    if hasattr(value, "tzinfo") and getattr(value, "tzinfo", None) is not None:
        try:
            value = value.replace(tzinfo=None)
        except Exception:
            pass

    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return None
        return value

    if isinstance(value, (dict, list, tuple, set)):
        value = json.dumps(_make_serializable(value), ensure_ascii=False)
    elif not isinstance(value, (str, int, bool)):
        value = str(value)

    if isinstance(value, str):
        cleaned = _ILLEGAL_EXCEL_CHARS_RE.sub("", value)
        if len(cleaned) > _EXCEL_MAX_CELL_LEN:
            cleaned = cleaned[:_EXCEL_MAX_CELL_LEN]
        return cleaned

    return value


def _build_excel_safe_dataframe(df):
    """Create a defensive copy of dataframe suitable for Excel export."""
    import pandas as pd

    safe_df = df.copy()

    for col in safe_df.columns:
        ser = safe_df[col]

        if pd.api.types.is_datetime64tz_dtype(ser.dtype):
            safe_df[col] = ser.dt.tz_localize(None)
            continue

        if pd.api.types.is_numeric_dtype(ser.dtype):
            safe_df[col] = ser.replace([float("inf"), float("-inf")], None)
            continue

        safe_df[col] = ser.map(_sanitize_excel_cell)

    return safe_df


def _build_excel_tz_unaware_dataframe(df):
    """Strip timezone info from datetime-like values prior to Excel export."""
    import pandas as pd

    safe_df = df.copy()

    for col in safe_df.columns:
        ser = safe_df[col]

        if pd.api.types.is_datetime64tz_dtype(ser.dtype):
            safe_df[col] = ser.dt.tz_localize(None)
            continue

        if pd.api.types.is_object_dtype(ser.dtype):
            safe_df[col] = ser.map(_sanitize_excel_cell)

    return safe_df


def _export_excel_with_fallback(df, excel_path: str, task_id: str) -> Optional[str]:
    """Export Excel and return warning text if fallback/degrade path was needed."""
    try:
        excel_tz_safe_df = _build_excel_tz_unaware_dataframe(df)
        excel_tz_safe_df.to_excel(excel_path, index=False, engine="openpyxl")
        return None
    except Exception as first_exc:
        logger.warning(
            "Task %s: initial Excel export failed, retrying with sanitized data: %s",
            task_id,
            first_exc,
        )

    try:
        excel_safe_df = _build_excel_safe_dataframe(_build_excel_tz_unaware_dataframe(df))
        excel_safe_df.to_excel(excel_path, index=False, engine="openpyxl")
        return "Excel export required sanitization for unsupported values."
    except Exception as final_exc:
        logger.exception(
            "Task %s: Excel export failed after sanitization: %s",
            task_id,
            final_exc,
        )
        return f"Excel export failed and was skipped: {final_exc}"


@celery_app.task(bind=True)
def process_csv_task(
    self,
    file_path: str,
    filename: str,
    config: Dict[str, Any],
) -> Dict[str, Any]:
    """Run the full MLHandler pipeline asynchronously.

    Args:
        file_path: Path to the uploaded CSV file on disk.
        filename: Original upload filename.
        config: User-selected cleaning options dict.

    Returns:
        Task completion payload with report and download URLs.
    """
    tracker = LineageTracker()

    try:
        _update(5, "Loading CSV")
        logger.info("Task %s: loading CSV from upload (%s)", self.request.id, filename)

        from app.services.loader import load_csv

        encodings: Optional[list] = None
        try:
            from app.core import config as core_config

            encodings = getattr(core_config, "DEFAULT_ENCODING", None)
        except Exception:
            pass

        loaded = load_csv(file_path, encodings=encodings)

        if isinstance(loaded, tuple) and len(loaded) == 2:
            df, load_meta = loaded
        else:
            df = loaded
            load_meta = {"encoding": None}

        tracker.log(
            action="file_loaded",
            reason="User upload",
            count=len(df),
            details={"columns": list(df.columns), "rows": len(df)},
        )

        sample_before: Dict[str, list] = {}
        for col in df.columns:
            sample_before[str(col)] = df[col].head(10).tolist()

        _update(20, "Mapping fields")

        field_mapping_stats: Dict[str, Any] = {}
        if config and "schema" in config:
            from app.services.field_mapper import FieldMapper

            mapper = FieldMapper(config.get("schema"))
            mapping_report = mapper.get_mapping_report(df)
            field_mapping_stats = {
                "total_columns": mapping_report["total_columns"],
                "mapped_count": len(mapping_report["mappings"]),
                "unmapped_count": len(mapping_report["unmapped"]),
                "mappings": mapping_report["mappings"],
                "unmapped": mapping_report["unmapped"],
            }
            df = mapper.map_fields(df)

        _update(40, "Inferring and fixing column types")

        from app.services.type_fixer import fix_types

        df, type_stats = fix_types(df, config)
        converted_count = int(type_stats.get("converted_count", 0) or 0)
        if converted_count == 0:
            conversions = type_stats.get("conversions", {}) if isinstance(type_stats, dict) else {}
            for info in conversions.values():
                if isinstance(info, dict):
                    converted_count += int(info.get("converted_count", 0) or 0)

        tracker.log(
            action="types_converted",
            reason="Automatic type inference",
            count=converted_count,
            details=type_stats,
        )

        _update(55, "Cleaning data")

        from app.services.cleaner import clean_dataframe

        df, clean_stats = clean_dataframe(df, overrides=config)
        nulls_filled = int(clean_stats.get("nulls_filled", 0) or 0)
        if nulls_filled == 0:
            nulls_filled = int(sum((clean_stats.get("imputed", {}) or {}).values()))

        tracker.log(
            action="nulls_imputed",
            reason="Missing value strategy: " + str(config.get("missing_strategy", "median/mode")),
            count=nulls_filled,
            details=clean_stats,
        )

        if config.get("remove_outliers"):
            _update(62, "Removing outliers")
            from app.services.outlier_detector import remove_outliers_iqr

            rows_before = len(df)
            numeric_cols = df.select_dtypes(include=["number"]).columns
            df = remove_outliers_iqr(df)
            tracker.log(
                action="outliers_removed",
                reason="IQR method, threshold 1.5",
                count=rows_before - len(df),
                details={"columns_affected": list(numeric_cols)},
            )
        else:
            tracker.log(
                action="outliers_removed",
                reason="Skipped because remove_outliers was disabled",
                count=0,
                details={"enabled": False},
            )

        _update(70, "Validating dataset")

        from app.services.validator import validate_dataframe, validate_schema

        df, rename_map = validate_dataframe(df)
        rename_log = [{"from": k, "to": v} for k, v in rename_map.items()]
        tracker.log(
            action="columns_renamed",
            reason="Duplicate column name resolution",
            count=len(rename_log),
            details=rename_log,
        )

        schema_validation: list = []
        if config and "schema" in config:
            schema_validation = validate_schema(df, config.get("schema"))

        _update(80, "Generating profiling report")

        from app.services.semantic_inferencer import infer_semantic_types
        from app.services.profiler import profile_dataframe

        semantic_types = infer_semantic_types(df)
        profile = profile_dataframe(df, semantic_types=semantic_types)

        sample_after: Dict[str, list] = {}
        for col in df.columns:
            sample_after[str(col)] = df[col].head(10).tolist()

        _update(90, "Building report")

        from app.services.reporter import generate_report

        report = generate_report(
            clean_stats=clean_stats,
            type_stats=type_stats,
            rename_map=rename_map,
        )
        report["profile"] = profile
        report["sample_before"] = sample_before
        report["sample_after"] = sample_after
        report["schema_validation"] = schema_validation
        report["field_mapping"] = field_mapping_stats
        report["load_metadata"] = load_meta
        report["semantic_types"] = semantic_types

        try:
            from app.core import config as core_config

            config_used = core_config.get_cleaning_rules(config)
        except Exception:
            config_used = config or {}
        report["config_used"] = config_used

        _update(95, "Saving outputs")

        task_id = self.request.id
        csv_path = os.path.join(DOWNLOADS_DIR, f"{task_id}.csv")
        report_path = os.path.join(DOWNLOADS_DIR, f"{task_id}_report.json")
        lineage_path = os.path.join(DOWNLOADS_DIR, f"{task_id}_lineage.json")
        parquet_path = os.path.join(DOWNLOADS_DIR, f"{task_id}.parquet")
        excel_path = os.path.join(DOWNLOADS_DIR, f"{task_id}.xlsx")

        df.to_csv(csv_path, index=False, encoding="utf-8")
        df.to_parquet(parquet_path, index=False, engine="pyarrow")
        excel_warning = _export_excel_with_fallback(df, excel_path, str(task_id))
        if excel_warning:
            report.setdefault("warnings", []).append(excel_warning)

        serializable_report = _make_serializable(report)
        with open(report_path, "w", encoding="utf-8") as fh:
            json.dump(serializable_report, fh, ensure_ascii=False)

        with open(lineage_path, "w", encoding="utf-8") as fh:
            fh.write(tracker.to_json())

        logger.info("Task %s: saved outputs in %s", self.request.id, DOWNLOADS_DIR)

        return {
            "status": "done",
            "percent": 100,
            "stage": "Complete",
            "download_url": f"/download/{task_id}",
            "download_url_parquet": f"/download/{task_id}?format=parquet",
            "download_url_excel": f"/download/{task_id}?format=excel",
            "lineage_url": f"/lineage/{task_id}",
            "report": serializable_report,
        }
    finally:
        try:
            os.remove(file_path)
        except Exception:
            pass
