# DEPRECATED: This synchronous pipeline is kept for reference only.
# All processing now goes through app/tasks.py via Celery.
# Do not call this module from any route or endpoint.

"""Minimal processing pipeline for MLHandler MVP.

This module orchestrates loading, type fixing, cleaning, validation,
profiling, and report generation. It exposes `process_csv(path)` which
returns a tuple `(cleaned_csv_path, report_dict)`.

The implementation intentionally keeps business logic minimal and
reuses service modules.
"""
from typing import Tuple, Dict, Any, Optional
import os
import tempfile

from app.services.loader import load_csv
from app.services.field_mapper import FieldMapper
from app.services.type_fixer import fix_types
from app.services.cleaner import clean_dataframe
from app.services.validator import validate_dataframe, validate_schema
from app.services.profiler import profile_dataframe
from app.services.reporter import generate_report
from app.core.logger import get_logger

logger = get_logger("services.pipeline")


def process_csv(input_path: str, config: Optional[Dict[str, Any]] = None) -> Tuple[str, Dict[str, Any]]:
    """Process the CSV at `input_path` and return cleaned file path and report.

    Returns:
        (cleaned_csv_path, report_dict)
    """
    # Try to obtain default encodings from config if available
    encodings: Optional[list] = None
    try:
        from app.core import config as core_config

        encodings = getattr(core_config, "DEFAULT_ENCODING", None)
    except Exception:
        encodings = None

    logger.info("Pipeline start for %s", input_path)
    # Load (loader may return (df, metadata) or just df)
    try:
        loaded = load_csv(input_path, encodings=encodings)
    except Exception:
        logger.exception("Failed loading CSV %s", input_path)
        raise
    if isinstance(loaded, tuple) and len(loaded) == 2:
        df, load_meta = loaded
    else:
        df = loaded
        load_meta = {"encoding": None}

    # Capture sample before processing (optimized)
    sample_before = df.head(10).to_dict(orient='list')

    # Field mapping (standardize column names if schema provided)
    field_mapping_stats = {}
    if config and "schema" in config:
        logger.info("Applying field mapping using schema")
        mapper = FieldMapper(config.get("schema"))
        
        # Get mapping report before applying
        mapping_report = mapper.get_mapping_report(df)
        field_mapping_stats = {
            "total_columns": mapping_report["total_columns"],
            "mapped_count": len(mapping_report["mappings"]),
            "unmapped_count": len(mapping_report["unmapped"]),
            "mappings": mapping_report["mappings"],
            "unmapped": mapping_report["unmapped"]
        }
        
        # Apply field mapping
        df = mapper.map_fields(df)
        logger.info("Field mapping complete: %d columns mapped, %d unmapped", 
                   field_mapping_stats["mapped_count"], 
                   field_mapping_stats["unmapped_count"])
    else:
        logger.info("No schema provided, skipping field mapping")

    # Type fixing (respect config if provided)
    df_fixed, type_stats = fix_types(df, config)

    # Cleaning (pass config overrides if provided)
    df_cleaned, clean_stats = clean_dataframe(df_fixed, overrides=config)

    # Validation (may raise DatasetValidationError)
    df_validated, rename_map = validate_dataframe(df_cleaned)

    # Capture sample after processing (optimized)
    sample_after = df_validated.head(10).to_dict(orient='list')

    # Profiling
    profile = profile_dataframe(df_validated)

    # Schema validation (if schema config provided)
    schema_validation = []
    if config and "schema" in config:
        schema_validation = validate_schema(df_validated, config.get("schema"))
        logger.info("Schema validation completed: %d issues found", len(schema_validation))

    # Report
    report = generate_report(clean_stats=clean_stats, type_stats=type_stats, rename_map=rename_map)
    report["profile"] = profile
    report["sample_before"] = sample_before
    report["sample_after"] = sample_after
    report["schema_validation"] = schema_validation
    report["field_mapping"] = field_mapping_stats  # Add field mapping stats
    # Include loader metadata (rows/columns/encoding) when available
    report["load_metadata"] = load_meta
    # Include the validated/merged cleaning config used for this run
    try:
        from app.core import config as core_config

        config_used = core_config.get_cleaning_rules(config)
    except Exception:
        config_used = config or {}
    report["config_used"] = config_used

    # Persist cleaned CSV to a temporary file and return its path
    fd, out_path = tempfile.mkstemp(suffix=".csv")
    os.close(fd)
    # Save as UTF-8
    df_validated.to_csv(out_path, index=False, encoding="utf-8")
    logger.info("Pipeline finished for %s; cleaned saved to %s", input_path, out_path)
    return out_path, report
