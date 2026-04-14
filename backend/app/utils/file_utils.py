"""File utilities for MLHandler.

Provides helpers to save uploaded files with UUID filenames, ensure the
uploads folder exists, and remove old files.
"""
from __future__ import annotations

import os
import uuid
import shutil
import time
from pathlib import Path
from typing import Optional


def ensure_uploads_dir(uploads_dir: Optional[str] = None) -> Path:
    """Ensure uploads directory exists and return its Path.

    By default creates `backend/app/temp/uploads` relative to project root.
    """
    if uploads_dir:
        p = Path(uploads_dir)
    else:
        p = Path(__file__).resolve().parents[1] / "temp" / "uploads"
    p.mkdir(parents=True, exist_ok=True)
    return p


def _safe_extension(original_filename: Optional[str]) -> str:
    if not original_filename:
        return ""
    suf = Path(original_filename).suffix
    # sanitize extension
    if suf and all(c.isalnum() or c == "." for c in suf):
        return suf
    return ""


def cleanup_old_files(uploads_dir: Optional[str] = None, max_age_seconds: int = 60 * 60 * 24) -> int:
    """Delete files older than `max_age_seconds` in the uploads dir.

    Returns the number of files deleted.
    """
    d = ensure_uploads_dir(uploads_dir)
    now = time.time()
    deleted = 0
    for p in d.iterdir():
        try:
            if not p.is_file():
                continue
            mtime = p.stat().st_mtime
            if now - mtime > max_age_seconds:
                p.unlink()
                deleted += 1
        except Exception:
            # ignore errors for individual files
            continue
    return deleted


def save_upload_with_uuid(content: bytes, original_filename: Optional[str] = None, uploads_dir: Optional[str] = None, max_age_seconds: int = 60 * 60 * 24) -> str:
    """Save bytes `content` to the uploads directory using a UUID filename.

    - Creates the uploads directory if it doesn't exist.
    - Runs a cleanup pass deleting files older than `max_age_seconds`.

    Returns the absolute path to the saved file as a string.
    """
    d = ensure_uploads_dir(uploads_dir)

    # Do a cleanup pass (best-effort)
    try:
        cleanup_old_files(str(d), max_age_seconds=max_age_seconds)
    except Exception:
        pass

    ext = _safe_extension(original_filename)
    fname = f"{uuid.uuid4().hex}{ext}"
    out_path = d / fname

    # Write atomically via temporary file
    tmp = out_path.with_suffix(out_path.suffix + ".tmp")
    with tmp.open("wb") as f:
        f.write(content)
    shutil.move(str(tmp), str(out_path))

    return str(out_path.resolve())


def save_upload_fileobj(fileobj, original_filename: Optional[str] = None, uploads_dir: Optional[str] = None, max_age_seconds: int = 60 * 60 * 24) -> str:
    """Save a file-like object (binary) to uploads dir and return path.

    The `fileobj` must implement a synchronous `.read()` method that returns bytes.
    """
    data = fileobj.read()
    return save_upload_with_uuid(data, original_filename=original_filename, uploads_dir=uploads_dir, max_age_seconds=max_age_seconds)
