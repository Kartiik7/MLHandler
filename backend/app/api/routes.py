from typing import Any, Dict, Optional
import asyncio
import json
import math
import os

import pandas as pd
from celery.result import AsyncResult
from fastapi import APIRouter, File, Form, HTTPException, UploadFile, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from starlette.responses import JSONResponse

from app.celery_app import celery_app
from app.core.config import DOWNLOADS_DIR, MAX_UPLOAD_SIZE, TEMP_DIR
from app.core.logger import get_logger
from app.utils.file_utils import save_upload_with_uuid

router = APIRouter()
logger = get_logger("api.routes")

os.makedirs(DOWNLOADS_DIR, exist_ok=True)


def _sanitize_json(value: Any) -> Any:
    if isinstance(value, dict):
        return {k: _sanitize_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_sanitize_json(v) for v in value]
    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return None
        return value
    return value


@router.post("/upload")
async def upload_csv(
    file: UploadFile = File(...),
    config: str = Form("{}"),
    pipeline_yaml: Optional[str] = Form(None),
) -> JSONResponse:
    """Accept CSV upload, enqueue Celery task, and return task_id immediately."""
    allowed_types = {"text/csv", "application/csv", "text/plain"}
    if file.content_type not in allowed_types and not (file.filename or "").lower().endswith(".csv"):
        raise HTTPException(status_code=400, detail="Uploaded file must be CSV")

    if pipeline_yaml:
        from app.services.pipeline_config import parse_pipeline_yaml, pipeline_steps_to_config

        steps = parse_pipeline_yaml(pipeline_yaml)
        config_dict = pipeline_steps_to_config(steps)
    else:
        try:
            config_dict = json.loads(config or "{}")
        except Exception as exc:
            raise HTTPException(status_code=400, detail=f"Invalid config JSON: {exc}")

    try:
        file_bytes = await file.read()
    finally:
        await file.close()

    if len(file_bytes) > int(MAX_UPLOAD_SIZE):
        raise HTTPException(status_code=413, detail="Uploaded file too large")

    file_path = save_upload_with_uuid(
        file_bytes,
        original_filename=file.filename,
        uploads_dir=TEMP_DIR,
    )

    try:
        from app.tasks import process_csv_task

        task = process_csv_task.delay(file_path, file.filename or "upload.csv", config_dict)
    except Exception as exc:
        try:
            os.remove(file_path)
        except Exception:
            pass
        logger.exception("Failed to enqueue processing task: %s", exc)
        raise HTTPException(status_code=503, detail="Task queue unavailable — is Redis running?")

    return JSONResponse({"task_id": task.id, "status": "queued"})


@router.get("/download/{task_id}")
async def download_file(task_id: str, format: str = "csv") -> FileResponse:
    """Download processed output in CSV, Parquet, or Excel format."""
    format_map = {
        "csv": (f"{task_id}.csv", "text/csv", "cleaned.csv"),
        "parquet": (f"{task_id}.parquet", "application/octet-stream", "cleaned.parquet"),
        "excel": (
            f"{task_id}.xlsx",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "cleaned.xlsx",
        ),
    }

    if format not in format_map:
        return JSONResponse(status_code=400, content={"error": f"Unknown format: {format}"})

    filename, media_type, download_name = format_map[format]
    path = os.path.join(DOWNLOADS_DIR, filename)
    if not os.path.exists(path):
        return JSONResponse(status_code=404, content={"error": "File not found"})

    return FileResponse(path=path, media_type=media_type, filename=download_name)


def _get_task_info(task_id: str) -> dict:
    result = AsyncResult(task_id, app=celery_app)
    return {
        "state": result.state,
        "info": result.info,
        "result": result.result if result.state == "SUCCESS" else None,
    }

@router.websocket("/ws/{task_id}")
async def websocket_progress(websocket: WebSocket, task_id: str) -> None:
    """Stream task progress updates over WebSocket."""
    await websocket.accept()
    try:
        while True:
            res = await asyncio.to_thread(_get_task_info, task_id)
            state = res["state"]

            if state == "PENDING":
                await websocket.send_json({"percent": 0, "stage": "Queued", "status": "pending"})

            elif state == "STARTED":
                await websocket.send_json({"percent": 2, "stage": "Starting", "status": "processing"})

            elif state == "PROGRESS":
                meta = res["info"] or {}
                await websocket.send_json(
                    {
                        "percent": meta.get("percent", 0),
                        "stage": meta.get("stage", "Processing"),
                        "status": "processing",
                    }
                )

            elif state == "SUCCESS":
                final = res["result"] or {}
                payload = {"percent": 100, "stage": "Complete", "status": "done"}
                if isinstance(final, dict):
                    payload.update(final)
                await websocket.send_json(payload)
                break

            elif state == "FAILURE":
                await websocket.send_json(
                    {
                        "percent": 0,
                        "stage": "Failed",
                        "status": "error",
                        "error": str(res["info"]),
                    }
                )
                break

            await asyncio.sleep(0.5)

    except WebSocketDisconnect:
        logger.info("WebSocket client disconnected for task %s", task_id)
    except Exception as exc:
        logger.exception("WebSocket error for task %s: %s", task_id, exc)
        try:
            await websocket.send_json({"status": "error", "error": str(exc)})
        except Exception:
            pass


@router.get("/lineage/{task_id}")
async def get_lineage(task_id: str) -> JSONResponse:
    """Return lineage JSON generated for a processed task."""
    lineage_path = os.path.join(DOWNLOADS_DIR, f"{task_id}_lineage.json")
    if not os.path.exists(lineage_path):
        return JSONResponse(status_code=404, content={"error": "Lineage not found"})

    with open(lineage_path, "r", encoding="utf-8") as fh:
        return JSONResponse(content=_sanitize_json(json.load(fh)))


@router.get("/column-stats/{task_id}")
async def column_stats(task_id: str) -> JSONResponse:
    """Analyze columns from a cleaned CSV and return statistics."""
    csv_path = os.path.join(DOWNLOADS_DIR, f"{task_id}.csv")
    if not os.path.exists(csv_path):
        raise HTTPException(status_code=404, detail="Processed file not found")

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        logger.exception("Failed to load CSV from %s: %s", csv_path, exc)
        raise HTTPException(status_code=500, detail=f"Failed to load CSV: {str(exc)}")

    semantic_types: Dict[str, str] = {}
    report_path = os.path.join(DOWNLOADS_DIR, f"{task_id}_report.json")
    if os.path.exists(report_path):
        try:
            with open(report_path, "r", encoding="utf-8") as fh:
                report_obj = json.load(fh)
                semantic_types = report_obj.get("semantic_types", {}) or {}
        except Exception:
            semantic_types = {}

    stats: Dict[str, Dict[str, Any]] = {}
    for col in df.columns:
        col_data = df[col]
        col_name = str(col)
        null_count = int(col_data.isna().sum())
        is_numeric = pd.api.types.is_numeric_dtype(col_data)

        if is_numeric:
            non_null_data = pd.to_numeric(col_data, errors="coerce").dropna()
            if len(non_null_data) > 0:
                q1 = float(non_null_data.quantile(0.25))
                median = float(non_null_data.quantile(0.50))
                q3 = float(non_null_data.quantile(0.75))
                iqr = q3 - q1
                whisker_low = q1 - 1.5 * iqr
                whisker_high = q3 + 1.5 * iqr
                outlier_count = int(((non_null_data < whisker_low) | (non_null_data > whisker_high)).sum())

                stats[col_name] = {
                    "type": "numeric",
                    "min": float(non_null_data.min()),
                    "max": float(non_null_data.max()),
                    "mean": float(non_null_data.mean()),
                    "std": float(non_null_data.std()) if len(non_null_data) > 1 else 0.0,
                    "q1": q1,
                    "median": median,
                    "q3": q3,
                    "iqr": iqr,
                    "whisker_low": whisker_low,
                    "whisker_high": whisker_high,
                    "outlier_count": outlier_count,
                    "null_count": null_count,
                    "semantic_type": semantic_types.get(col_name, "UNKNOWN"),
                }
            else:
                stats[col_name] = {
                    "type": "numeric",
                    "min": None,
                    "max": None,
                    "mean": None,
                    "std": None,
                    "q1": None,
                    "median": None,
                    "q3": None,
                    "iqr": None,
                    "whisker_low": None,
                    "whisker_high": None,
                    "outlier_count": 0,
                    "null_count": null_count,
                    "semantic_type": semantic_types.get(col_name, "UNKNOWN"),
                }
        else:
            value_counts = col_data.value_counts()
            top_5_values = [{"value": str(val), "count": int(count)} for val, count in value_counts.head(5).items()]
            stats[col_name] = {
                "type": "categorical",
                "unique_count": int(len(value_counts)),
                "top_5_values": top_5_values,
                "null_count": null_count,
                "semantic_type": semantic_types.get(col_name, "UNKNOWN"),
            }

    response_data = {
        "file_path": csv_path,
        "total_rows": int(df.shape[0]),
        "total_columns": int(df.shape[1]),
        "columns": stats,
        "semantic_types": semantic_types,
    }
    return JSONResponse(content=response_data)


@router.get("/histogram/{task_id}/{column}")
async def column_histogram(task_id: str, column: str, bins: int = 10) -> JSONResponse:
    """Compute and return a real histogram for a numeric column in a processed CSV.

    Query params:
        bins: number of histogram bins (2–100, default 10)

    Returns:
        {
            "column": str,
            "bins": list[str],      # human-readable bin labels  "lo–hi"
            "counts": list[int],    # actual row counts per bin
            "bin_width": float,
            "total_non_null": int,
            "min": float,
            "max": float,
        }
    """
    import numpy as np

    # Validate bins parameter
    if not (2 <= bins <= 100):
        return JSONResponse(
            status_code=422,
            content={"error": "bins must be between 2 and 100"},
        )

    csv_path = os.path.join(DOWNLOADS_DIR, f"{task_id}.csv")
    if not os.path.exists(csv_path):
        return JSONResponse(status_code=404, content={"error": "Processed file not found"})

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        logger.exception("histogram: failed to load CSV %s: %s", csv_path, exc)
        return JSONResponse(status_code=500, content={"error": f"Failed to load CSV: {exc}"})

    if column not in df.columns:
        return JSONResponse(
            status_code=400,
            content={"error": f"Column '{column}' not found in dataset"},
        )

    col_series = df[column]

    if not pd.api.types.is_numeric_dtype(col_series):
        return JSONResponse(
            status_code=400,
            content={
                "error": f"Column '{column}' is not numeric (dtype: {col_series.dtype})"
            },
        )

    # Drop NaN and infinite values before computing histogram
    numeric_values = pd.to_numeric(col_series, errors="coerce")
    numeric_values = numeric_values.replace([float("inf"), float("-inf")], pd.NA).dropna()
    total_non_null = int(len(numeric_values))

    if total_non_null == 0:
        # All values were null or non-numeric — return empty histogram
        return JSONResponse(
            content={
                "column": column,
                "bins": [],
                "counts": [],
                "bin_width": 0.0,
                "total_non_null": 0,
                "min": None,
                "max": None,
            }
        )

    col_min = float(numeric_values.min())
    col_max = float(numeric_values.max())

    # Handle zero-range column (all values identical)
    if col_min == col_max:
        return JSONResponse(
            content={
                "column": column,
                "bins": [f"{col_min:.4g}"],
                "counts": [total_non_null],
                "bin_width": 0.0,
                "total_non_null": total_non_null,
                "min": col_min,
                "max": col_max,
            }
        )

    # Compute actual histogram using numpy
    np_values = numeric_values.to_numpy(dtype=float)
    counts_array, edges_array = np.histogram(np_values, bins=bins)

    bin_width = float(edges_array[1] - edges_array[0])

    # Build human-readable bin labels "lo–hi"
    bin_labels: list[str] = []
    for i in range(len(edges_array) - 1):
        lo = edges_array[i]
        hi = edges_array[i + 1]
        # Choose precision based on bin width
        if bin_width >= 1:
            label = f"{lo:.2f}–{hi:.2f}"
        else:
            # More decimal places for small ranges
            precision = max(2, -int(np.floor(np.log10(bin_width))) + 1)
            fmt = f"{{:.{precision}f}}"
            label = f"{fmt.format(lo)}–{fmt.format(hi)}"
        bin_labels.append(label)

    return JSONResponse(
        content={
            "column": column,
            "bins": bin_labels,
            "counts": [int(c) for c in counts_array],
            "bin_width": round(bin_width, 6),
            "total_non_null": total_non_null,
            "min": col_min,
            "max": col_max,
        }
    )


@router.get("/correlation/{task_id}")
async def column_correlation_matrix(task_id: str, max_columns: int = 25) -> JSONResponse:
    """Return Pearson correlation matrix for numeric columns of a processed CSV.

    Query params:
        max_columns: Maximum number of numeric columns to include (2–60, default 25).
                     When the CSV has more numeric columns than this limit, the top-N
                     by variance are kept and the response includes `sampled=true`.

    Returns:
        {
            "columns":               list[str]        — ordered column names
            "matrix":                list[list[float|null]] — N×N correlation values
            "total_numeric_columns": int              — actual numeric col count in CSV
            "sampled":               bool             — true when columns were trimmed
            "dropped_columns":       list[str]        — columns excluded by variance trim
            "row_sampled":           bool             — true when rows were randomly sampled
            "sample_rows":           int | null       — row count used if row_sampled
            "method":                "pearson"
        }
    """
    import numpy as np

    # ── Validate query param ──────────────────────────────────────────────────
    if not (2 <= max_columns <= 60):
        return JSONResponse(
            status_code=422,
            content={"error": "max_columns must be between 2 and 60"},
        )

    # ── Try loading pre-computed correlation from report JSON ─────────────────
    report_path = os.path.join(DOWNLOADS_DIR, f"{task_id}_report.json")
    if os.path.exists(report_path):
        try:
            with open(report_path, "r", encoding="utf-8") as fh:
                report = json.load(fh)
            pre_computed = report.get("profile", {}).get("correlation")
            if pre_computed:
                if len(pre_computed.get("columns", [])) > max_columns:
                    cols = pre_computed["columns"][:max_columns]
                    matrix = [row[:max_columns] for row in pre_computed["matrix"][:max_columns]]
                    trimmed = {
                        "columns": cols,
                        "matrix": matrix,
                        "total_numeric_columns": pre_computed.get("total_numeric_columns"),
                        "sampled": True,
                        "dropped_columns": pre_computed.get("dropped_columns", []) + pre_computed["columns"][max_columns:],
                        "row_sampled": pre_computed.get("row_sampled"),
                        "sample_rows": pre_computed.get("sample_rows"),
                        "method": "pearson"
                    }
                    return JSONResponse(content=_sanitize_json(trimmed))
                return JSONResponse(content=_sanitize_json(pre_computed))
        except Exception as exc:
            logger.warning("Failed to load pre-computed correlation from report for task %s: %s", task_id, exc)

    # ── Load processed CSV ────────────────────────────────────────────────────
    csv_path = os.path.join(DOWNLOADS_DIR, f"{task_id}.csv")
    if not os.path.exists(csv_path):
        return JSONResponse(status_code=404, content={"error": "Processed file not found"})

    try:
        df = pd.read_csv(csv_path)
    except Exception as exc:
        logger.exception("correlation: failed to load CSV %s: %s", csv_path, exc)
        return JSONResponse(status_code=500, content={"error": f"Failed to load CSV: {exc}"})

    # ── Select numeric columns only ───────────────────────────────────────────
    numeric_df = df.select_dtypes(include="number")
    total_numeric = int(numeric_df.shape[1])

    if total_numeric < 2:
        return JSONResponse(
            status_code=400,
            content={
                "error": (
                    f"Fewer than 2 numeric columns found ({total_numeric}). "
                    "Pearson correlation requires at least 2 numeric columns."
                )
            },
        )

    # ── Column-count guard: keep top-N by variance ────────────────────────────
    sampled = False
    dropped_columns: list[str] = []

    if total_numeric > max_columns:
        sampled = True
        variances = numeric_df.var(numeric_only=True).sort_values(ascending=False)
        keep = variances.index[:max_columns].tolist()
        dropped_columns = variances.index[max_columns:].tolist()
        numeric_df = numeric_df[keep]
        logger.info(
            "correlation: trimmed %d → %d columns by variance for task %s",
            total_numeric,
            max_columns,
            task_id,
        )

    # ── Row-count guard: sample for very large datasets ───────────────────────
    _ROW_SAMPLE_THRESHOLD = 500_000
    _ROW_SAMPLE_SIZE = 100_000
    row_sampled = False
    sample_rows: int | None = None

    if len(numeric_df) > _ROW_SAMPLE_THRESHOLD:
        numeric_df = numeric_df.sample(n=_ROW_SAMPLE_SIZE, random_state=42)
        row_sampled = True
        sample_rows = _ROW_SAMPLE_SIZE
        logger.info(
            "correlation: row-sampled %d → %d for task %s",
            len(df),
            _ROW_SAMPLE_SIZE,
            task_id,
        )

    # ── Compute Pearson correlation ────────────────────────────────────────────
    try:
        corr_matrix = numeric_df.corr(method="pearson", numeric_only=True)
    except Exception as exc:
        logger.exception("correlation: corr() failed for task %s: %s", task_id, exc)
        return JSONResponse(status_code=500, content={"error": f"Correlation computation failed: {exc}"})

    columns: list[str] = [str(c) for c in corr_matrix.columns]

    # Serialise matrix: NaN (constant column pairs) → null, inf → null
    matrix: list[list[float | None]] = []
    for _, row in corr_matrix.iterrows():
        serialised_row: list[float | None] = []
        for val in row:
            if val is None or (isinstance(val, float) and (math.isnan(val) or math.isinf(val))):
                serialised_row.append(None)
            else:
                serialised_row.append(round(float(val), 4))
        matrix.append(serialised_row)

    return JSONResponse(
        content={
            "columns": columns,
            "matrix": matrix,
            "total_numeric_columns": total_numeric,
            "sampled": sampled,
            "dropped_columns": [str(c) for c in dropped_columns],
            "row_sampled": row_sampled,
            "sample_rows": sample_rows,
            "method": "pearson",
        }
    )


@router.get("/quality-score/{task_id}")
async def get_quality_score(task_id: str) -> JSONResponse:
    """Calculate and return a Data Quality Score and sub-scores for a processed task."""
    report_path = os.path.join(DOWNLOADS_DIR, f"{task_id}_report.json")
    csv_path = os.path.join(DOWNLOADS_DIR, f"{task_id}.csv")

    if not os.path.exists(report_path) or not os.path.exists(csv_path):
        return JSONResponse(status_code=404, content={"error": "Processed dataset files not found"})

    try:
        with open(report_path, "r", encoding="utf-8") as fh:
            report = json.load(fh)
        df = pd.read_csv(csv_path)
    except Exception as exc:
        logger.exception("Failed to load processed dataset files for task %s: %s", task_id, exc)
        return JSONResponse(status_code=500, content={"error": f"Failed to load dataset files: {exc}"})

    # 1. Missing Score
    missing_before = report.get("_raw_clean_stats", {}).get("missing_before", {}) or {}
    total_missing = sum(int(val) for val in missing_before.values())
    rows_before = int(report.get("rows_before", len(df)))
    columns_before = int(report.get("columns_before", len(df.columns)))
    total_cells = rows_before * columns_before
    missing_ratio = float(total_missing) / float(total_cells) if total_cells > 0 else 0.0
    missing_score = max(0, 100 - int(missing_ratio * 100 * 1.5))

    # 2. Duplicate Score
    duplicates = int(report.get("duplicates_removed", 0))
    duplicate_ratio = float(duplicates) / float(rows_before) if rows_before > 0 else 0.0
    duplicate_score = max(0, 100 - int(duplicate_ratio * 100 * 3.0))

    # 3. Outlier Score
    profile_columns = report.get("profile", {}).get("columns", {}) or {}
    total_outliers = 0
    numeric_col_count = 0
    rows_after = int(report.get("rows_after", len(df)))

    for col_summary in profile_columns.values():
        if col_summary.get("is_numeric"):
            numeric_col_count += 1
            total_outliers += int(col_summary.get("outlier_count", 0))

    total_numeric_cells = numeric_col_count * rows_after
    outlier_ratio = float(total_outliers) / float(total_numeric_cells) if total_numeric_cells > 0 else 0.0
    outlier_score = max(0, 100 - int(outlier_ratio * 100 * 2.0)) if numeric_col_count > 0 else 100

    # 4. Empty Column Score
    columns_dropped = report.get("columns_dropped", []) or []
    empty_ratio = float(len(columns_dropped)) / float(columns_before) if columns_before > 0 else 0.0
    empty_score = max(0, 100 - int(empty_ratio * 100 * 2.0))

    # 5. Type Score
    # Mixed type columns detection
    mixed_cols = []
    for col in df.columns:
        non_null = df[col].dropna()
        if len(non_null) > 0:
            type_names = non_null.map(lambda x: type(x).__name__).unique()
            if len(type_names) > 1:
                mixed_cols.append(str(col))

    mixed_count = len(mixed_cols)

    # Schema type issues
    schema_validation = report.get("schema_validation", []) or []
    schema_type_issue_count = 0
    for issue in schema_validation:
        issue_text = str(issue.get("issue", "")).lower()
        if "type" in issue_text or "expected" in issue_text:
            schema_type_issue_count += 1

    # Coercion failures (imputed NaNs in coerced columns)
    imputed = report.get("missing_filled_per_column", {}) or {}
    type_conversions = report.get("type_conversions", {}) or {}

    coercion_penalties = 0
    coercion_failures = {}
    for col, conv_info in type_conversions.items():
        action = conv_info.get("action")
        if action in ("to_numeric", "to_datetime", "unit_extraction"):
            col_imputed = int(imputed.get(col, 0))
            if col_imputed > 0 and rows_after > 0:
                failure_ratio = float(col_imputed) / float(rows_after)
                penalty = int(failure_ratio * 100 * 1.0)
                coercion_penalties += penalty
                coercion_failures[str(col)] = col_imputed

    total_type_penalty = mixed_count * 15 + schema_type_issue_count * 10 + coercion_penalties
    type_score = max(0, 100 - total_type_penalty)

    # 6. Composite Score
    quality_score = int(round(
        0.25 * missing_score +
        0.20 * duplicate_score +
        0.20 * outlier_score +
        0.20 * type_score +
        0.15 * empty_score
    ))

    response_data = {
        "quality_score": quality_score,
        "missing_score": missing_score,
        "duplicate_score": duplicate_score,
        "outlier_score": outlier_score,
        "type_score": type_score,
        "empty_score": empty_score,
        "details": {
            "missing": {
                "total_cells": int(total_cells),
                "missing_cells": int(total_missing),
                "missing_ratio": round(missing_ratio, 6),
            },
            "duplicates": {
                "total_rows": int(rows_before),
                "duplicates_removed": int(duplicates),
                "duplicate_ratio": round(duplicate_ratio, 6),
            },
            "outliers": {
                "total_numeric_cells": int(total_numeric_cells),
                "outlier_cells": int(total_outliers),
                "outlier_ratio": round(outlier_ratio, 6),
            },
            "types": {
                "mixed_type_columns": mixed_cols,
                "schema_type_issues": int(schema_type_issue_count),
                "coercion_failures": coercion_failures,
            },
            "empty_columns": {
                "total_columns": int(columns_before),
                "dropped_columns": [str(c) for c in columns_dropped],
                "empty_ratio": round(empty_ratio, 6),
            }
        }
    }

    return JSONResponse(content=_sanitize_json(response_data))


@router.get("/datasets")
async def list_datasets() -> JSONResponse:
    """Scan DOWNLOADS_DIR for processed dataset reports and return a history list."""
    from datetime import datetime
    if not os.path.exists(DOWNLOADS_DIR):
        return JSONResponse(content=[])

    datasets = []
    try:
        for filename in os.listdir(DOWNLOADS_DIR):
            if filename.endswith("_report.json"):
                task_id = filename[:-12]  # strip '_report.json'
                report_path = os.path.join(DOWNLOADS_DIR, filename)
                
                if os.path.exists(report_path):
                    try:
                        mtime = os.path.getmtime(report_path)
                        processed_at = datetime.utcfromtimestamp(mtime).isoformat() + "Z"
                        
                        with open(report_path, "r", encoding="utf-8") as fh:
                            report = json.load(fh)
                            
                        filename_original = report.get("load_metadata", {}).get("filename")
                        if not filename_original:
                            filename_original = f"dataset_{task_id[:8]}.csv"
                            
                        datasets.append({
                            "task_id": task_id,
                            "filename": filename_original,
                            "rows": int(report.get("rows_after", 0)),
                            "columns": int(report.get("columns_after", 0)),
                            "processed_at": processed_at,
                        })
                    except Exception as exc:
                        logger.warning("Failed to parse report %s: %s", filename, exc)
                        
        datasets.sort(key=lambda x: x["processed_at"], reverse=True)
    except Exception as exc:
        logger.exception("Failed to list datasets in history: %s", exc)
        return JSONResponse(status_code=500, content={"error": f"Failed to list history: {exc}"})

    return JSONResponse(content=datasets)


@router.get("/compare/{task_id_a}/{task_id_b}")
async def compare_datasets(task_id_a: str, task_id_b: str) -> JSONResponse:
    """Compare two processed datasets and return a detailed delta report."""
    # Load A
    report_path_a = os.path.join(DOWNLOADS_DIR, f"{task_id_a}_report.json")
    csv_path_a = os.path.join(DOWNLOADS_DIR, f"{task_id_a}.csv")
    # Load B
    report_path_b = os.path.join(DOWNLOADS_DIR, f"{task_id_b}_report.json")
    csv_path_b = os.path.join(DOWNLOADS_DIR, f"{task_id_b}.csv")

    if not all(os.path.exists(p) for p in (report_path_a, csv_path_a, report_path_b, csv_path_b)):
        return JSONResponse(status_code=404, content={"error": "One or both datasets were not found in history"})

    try:
        with open(report_path_a, "r", encoding="utf-8") as fh:
            report_a = json.load(fh)
        with open(report_path_b, "r", encoding="utf-8") as fh:
            report_b = json.load(fh)
            
        df_a = pd.read_csv(csv_path_a)
        df_b = pd.read_csv(csv_path_b)
    except Exception as exc:
        logger.exception("Failed to load datasets for comparison: %s", exc)
        return JSONResponse(status_code=500, content={"error": f"Failed to load dataset files: {exc}"})

    filename_a = report_a.get("load_metadata", {}).get("filename", f"dataset_{task_id_a[:8]}.csv")
    filename_b = report_b.get("load_metadata", {}).get("filename", f"dataset_{task_id_b[:8]}.csv")

    rows_a = int(report_a.get("rows_after", len(df_a)))
    rows_b = int(report_b.get("rows_after", len(df_b)))
    row_diff = rows_b - rows_a
    row_pct = (row_diff / rows_a * 100.0) if rows_a > 0 else 0.0

    cols_a = int(df_a.shape[1])
    cols_b = int(df_b.shape[1])
    col_diff = cols_b - cols_a

    set_a = set(df_a.columns)
    set_b = set(df_b.columns)
    added_cols = sorted(list(set_b - set_a))
    removed_cols = sorted(list(set_a - set_b))
    common_cols = set_a & set_b

    type_changes = {}
    for col in common_cols:
        dt_a = str(df_a[col].dtype)
        dt_b = str(df_b[col].dtype)
        if dt_a != dt_b:
            type_changes[str(col)] = {"a": dt_a, "b": dt_b}

    missing_diff = {}
    missing_before_a = report_a.get("_raw_clean_stats", {}).get("missing_before", {}) or {}
    missing_before_b = report_b.get("_raw_clean_stats", {}).get("missing_before", {}) or {}
    
    rows_before_a = int(report_a.get("rows_before", rows_a))
    rows_before_b = int(report_b.get("rows_before", rows_b))

    for col in common_cols:
        col_str = str(col)
        count_a = int(missing_before_a.get(col_str, 0))
        count_b = int(missing_before_b.get(col_str, 0))
        
        pct_a = count_a / rows_before_a if rows_before_a > 0 else 0.0
        pct_b = count_b / rows_before_b if rows_before_b > 0 else 0.0
        diff_pct = pct_b - pct_a
        
        if count_a > 0 or count_b > 0:
            missing_diff[col_str] = {
                "a_count": count_a,
                "a_percent": round(pct_a, 6),
                "b_count": count_b,
                "b_percent": round(pct_b, 6),
                "diff_percent": round(diff_pct, 6)
            }

    def calc_scores(report_obj, df_obj):
        missing_before = report_obj.get("_raw_clean_stats", {}).get("missing_before", {}) or {}
        total_missing = sum(int(val) for val in missing_before.values())
        r_before = int(report_obj.get("rows_before", len(df_obj)))
        c_before = int(report_obj.get("columns_before", len(df_obj.columns)))
        total_cells = r_before * c_before
        missing_ratio = float(total_missing) / float(total_cells) if total_cells > 0 else 0.0
        s_missing = max(0, 100 - int(missing_ratio * 100 * 1.5))

        duplicates = int(report_obj.get("duplicates_removed", 0))
        duplicate_ratio = float(duplicates) / float(r_before) if r_before > 0 else 0.0
        s_duplicate = max(0, 100 - int(duplicate_ratio * 100 * 3.0))

        profile_columns = report_obj.get("profile", {}).get("columns", {}) or {}
        total_outliers = 0
        numeric_col_count = 0
        r_after = int(report_obj.get("rows_after", len(df_obj)))

        for col_summary in profile_columns.values():
            if col_summary.get("is_numeric"):
                numeric_col_count += 1
                total_outliers += int(col_summary.get("outlier_count", 0))

        total_numeric_cells = numeric_col_count * r_after
        outlier_ratio = float(total_outliers) / float(total_numeric_cells) if total_numeric_cells > 0 else 0.0
        s_outlier = max(0, 100 - int(outlier_ratio * 100 * 2.0)) if numeric_col_count > 0 else 100

        columns_dropped = report_obj.get("columns_dropped", []) or []
        empty_ratio = float(len(columns_dropped)) / float(c_before) if c_before > 0 else 0.0
        s_empty = max(0, 100 - int(empty_ratio * 100 * 2.0))

        mixed_count = 0
        for col in df_obj.columns:
            non_null = df_obj[col].dropna()
            if len(non_null) > 0:
                type_names = non_null.map(lambda x: type(x).__name__).unique()
                if len(type_names) > 1:
                    mixed_count += 1

        schema_validation = report_obj.get("schema_validation", []) or []
        schema_type_issue_count = 0
        for issue in schema_validation:
            issue_text = str(issue.get("issue", "")).lower()
            if "type" in issue_text or "expected" in issue_text:
                schema_type_issue_count += 1

        imputed = report_obj.get("missing_filled_per_column", {}) or {}
        type_conversions = report_obj.get("type_conversions", {}) or {}

        coercion_penalties = 0
        for col, conv_info in type_conversions.items():
            action = conv_info.get("action")
            if action in ("to_numeric", "to_datetime", "unit_extraction"):
                col_imputed = int(imputed.get(col, 0))
                if col_imputed > 0 and r_after > 0:
                    failure_ratio = float(col_imputed) / float(r_after)
                    coercion_penalties += int(failure_ratio * 100 * 1.0)

        total_type_penalty = mixed_count * 15 + schema_type_issue_count * 10 + coercion_penalties
        s_type = max(0, 100 - total_type_penalty)

        s_quality = int(round(
            0.25 * s_missing +
            0.20 * s_duplicate +
            0.20 * s_outlier +
            0.20 * s_type +
            0.15 * s_empty
        ))

        return {
            "quality_score": s_quality,
            "missing_score": s_missing,
            "duplicate_score": s_duplicate,
            "outlier_score": s_outlier,
            "type_score": s_type,
            "empty_score": s_empty
        }

    scores_a = calc_scores(report_a, df_a)
    scores_b = calc_scores(report_b, df_b)

    quality_diff = {}
    for key in scores_a.keys():
        val_a = scores_a[key]
        val_b = scores_b[key]
        quality_diff[key] = {
            "a": val_a,
            "b": val_b,
            "diff": val_b - val_a
        }

    # 8. Numeric distribution changes
    distribution_diff = {}
    profile_a = report_a.get("profile", {}).get("columns", {}) or {}
    profile_b = report_b.get("profile", {}).get("columns", {}) or {}

    for col in common_cols:
        col_str = str(col)
        col_a = profile_a.get(col_str, {})
        col_b = profile_b.get(col_str, {})
        
        if col_a.get("is_numeric") and col_b.get("is_numeric"):
            mean_a = col_a.get("mean")
            mean_b = col_b.get("mean")
            std_a = col_a.get("std")
            std_b = col_b.get("std")
            min_a = col_a.get("min")
            min_b = col_b.get("min")
            max_a = col_a.get("max")
            max_b = col_b.get("max")
            median_a = col_a.get("median")
            median_b = col_b.get("median")
            
            def get_diff(val_a, val_b):
                if val_a is not None and val_b is not None:
                    return round(float(val_b) - float(val_a), 4)
                return None
                
            distribution_diff[col_str] = {
                "mean": {"a": mean_a, "b": mean_b, "diff": get_diff(mean_a, mean_b)},
                "std": {"a": std_a, "b": std_b, "diff": get_diff(std_a, std_b)},
                "min": {"a": min_a, "b": min_b, "diff": get_diff(min_a, min_b)},
                "max": {"a": max_a, "b": max_b, "diff": get_diff(max_a, max_b)},
                "median": {"a": median_a, "b": median_b, "diff": get_diff(median_a, median_b)},
            }

    # 9. Correlation changes
    correlation_diff = []
    corr_a = report_a.get("profile", {}).get("correlation", {}) or {}
    corr_b = report_b.get("profile", {}).get("correlation", {}) or {}

    cols_arr_a = corr_a.get("columns", [])
    matrix_arr_a = corr_a.get("matrix", [])
    cols_arr_b = corr_b.get("columns", [])
    matrix_arr_b = corr_b.get("matrix", [])

    if cols_arr_a and matrix_arr_a and cols_arr_b and matrix_arr_b:
        lookup_a = {}
        for r_idx, row_name in enumerate(cols_arr_a):
            for c_idx, col_name in enumerate(cols_arr_a):
                val = matrix_arr_a[r_idx][c_idx]
                if val is not None:
                    key = tuple(sorted([row_name, col_name]))
                    lookup_a[key] = val
                    
        lookup_b = {}
        for r_idx, row_name in enumerate(cols_arr_b):
            for c_idx, col_name in enumerate(cols_arr_b):
                val = matrix_arr_b[r_idx][c_idx]
                if val is not None:
                    key = tuple(sorted([row_name, col_name]))
                    lookup_b[key] = val
                    
        common_pairs = set(lookup_a.keys()) & set(lookup_b.keys())
        for pair in common_pairs:
            col1, col2 = pair
            if col1 != col2:
                r_a = lookup_a[pair]
                r_b = lookup_b[pair]
                diff = round(r_b - r_a, 4)
                correlation_diff.append({
                    "col1": str(col1),
                    "col2": str(col2),
                    "r_a": r_a,
                    "r_b": r_b,
                    "diff": diff
                })
        correlation_diff.sort(key=lambda x: abs(x["diff"]), reverse=True)

    response_data = {
        "dataset_a": {
            "task_id": task_id_a,
            "filename": filename_a,
        },
        "dataset_b": {
            "task_id": task_id_b,
            "filename": filename_b,
        },
        "row_diff": {
            "a": rows_a,
            "b": rows_b,
            "difference": row_diff,
            "percent_change": round(row_pct, 4)
        },
        "col_diff": {
            "a": cols_a,
            "b": cols_b,
            "difference": col_diff
        },
        "columns_added": added_cols,
        "columns_removed": removed_cols,
        "type_changes": type_changes,
        "missing_diff": missing_diff,
        "quality_diff": quality_diff,
        "distribution_diff": distribution_diff,
        "correlation_diff": correlation_diff
    }

    return JSONResponse(content=_sanitize_json(response_data))


@router.post("/api/outliers")
async def detect_outliers(payload: dict) -> JSONResponse:
    """Detect outliers from a processed file referenced by task_id."""
    task_id = payload.get("task_id")
    if not task_id:
        return JSONResponse(status_code=400, content={"error": "task_id required"})

    csv_path = os.path.join(DOWNLOADS_DIR, f"{task_id}.csv")
    if not os.path.exists(csv_path):
        return JSONResponse(status_code=404, content={"error": "Processed file not found"})

    from app.services.outlier_detector import detect_outliers_iqr, get_outlier_summary

    df = pd.read_csv(csv_path)
    outliers_dict = detect_outliers_iqr(df)
    summary = get_outlier_summary(df, outliers_dict)

    return JSONResponse(
        content={
            "total_rows": int(df.shape[0]),
            "total_columns": int(df.shape[1]),
            "numeric_columns_analyzed": len(outliers_dict),
            "outliers": summary,
        }
    )


@router.post("/api/remove-outliers")
async def remove_outliers_endpoint(payload: dict) -> JSONResponse:
    """Remove outliers from processed CSV by task_id and selected columns."""
    task_id = payload.get("task_id")
    columns = payload.get("columns", [])

    if not task_id:
        return JSONResponse(status_code=400, content={"error": "task_id required"})

    if columns is not None and not isinstance(columns, list):
        return JSONResponse(status_code=400, content={"error": "columns must be a list"})

    csv_path = os.path.join(DOWNLOADS_DIR, f"{task_id}.csv")
    if not os.path.exists(csv_path):
        return JSONResponse(status_code=404, content={"error": "File not found"})

    from app.services.outlier_detector import detect_outliers_iqr

    df = pd.read_csv(csv_path)
    outliers_dict = detect_outliers_iqr(df)

    target_columns = columns if columns else list(outliers_dict.keys())
    outlier_indices_set = set()
    for column in target_columns:
        if column in outliers_dict:
            outlier_indices_set.update(outliers_dict[column])

    outlier_indices = sorted(outlier_indices_set)
    if outlier_indices:
        cleaned_df = df.drop(index=outlier_indices).reset_index(drop=True)
    else:
        cleaned_df = df.copy()

    output_path = os.path.join(DOWNLOADS_DIR, f"{task_id}_no_outliers.csv")
    cleaned_df.to_csv(output_path, index=False)

    return JSONResponse(
        content={
            "download_url": f"/download-outlier-cleaned/{task_id}",
            "rows_removed": len(df) - len(cleaned_df),
            "original_rows": len(df),
            "cleaned_rows": len(cleaned_df),
        }
    )


@router.get("/download-outlier-cleaned/{task_id}")
async def download_outlier_cleaned(task_id: str) -> FileResponse:
    """Download CSV generated by the outlier-removal endpoint."""
    path = os.path.join(DOWNLOADS_DIR, f"{task_id}_no_outliers.csv")
    if not os.path.exists(path):
        return JSONResponse(status_code=404, content={"error": "Not found"})

    return FileResponse(path=path, media_type="text/csv", filename="cleaned_no_outliers.csv")


@router.post("/validate-yaml")
async def validate_yaml(payload: dict) -> JSONResponse:
    """Validate user-provided pipeline YAML and return normalized steps."""
    yaml_string = payload.get("yaml", "")
    try:
        from app.services.pipeline_config import parse_pipeline_yaml

        steps = parse_pipeline_yaml(yaml_string)
        return JSONResponse(content={"valid": True, "steps": steps})
    except ValueError as exc:
        return JSONResponse(status_code=400, content={"valid": False, "error": str(exc)})
