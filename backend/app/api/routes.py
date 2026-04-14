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


@router.websocket("/ws/{task_id}")
async def websocket_progress(websocket: WebSocket, task_id: str) -> None:
    """Stream task progress updates over WebSocket."""
    await websocket.accept()
    try:
        while True:
            result = AsyncResult(task_id, app=celery_app)

            if result.state == "PENDING":
                await websocket.send_json({"percent": 0, "stage": "Queued", "status": "pending"})

            elif result.state == "STARTED":
                await websocket.send_json({"percent": 2, "stage": "Starting", "status": "processing"})

            elif result.state == "PROGRESS":
                meta = result.info or {}
                await websocket.send_json(
                    {
                        "percent": meta.get("percent", 0),
                        "stage": meta.get("stage", "Processing"),
                        "status": "processing",
                    }
                )

            elif result.state == "SUCCESS":
                final = result.result or {}
                payload = {"percent": 100, "stage": "Complete", "status": "done"}
                if isinstance(final, dict):
                    payload.update(final)
                await websocket.send_json(payload)
                break

            elif result.state == "FAILURE":
                await websocket.send_json(
                    {
                        "percent": 0,
                        "stage": "Failed",
                        "status": "error",
                        "error": str(result.info),
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
