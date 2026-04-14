"""Simple constants configuration for MLHandler MVP.

This module contains constants and lightweight helpers used by the
application for upload handling, CSV processing defaults, and
cleaning-rule configuration.
"""

from typing import Optional, Dict, Any
import os
import json
from dotenv import load_dotenv

BACKEND_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
load_dotenv(os.path.join(BACKEND_ROOT, ".env"))

# Runtime settings
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
MAX_UPLOAD_SIZE = int(os.getenv("MAX_UPLOAD_SIZE_MB", "50")) * 1024 * 1024
DOWNLOADS_DIR = os.getenv("DOWNLOADS_DIR", "downloads")
TEMP_DIR = os.getenv("TEMP_DIR", "app/temp/uploads")
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
WORKERS = int(os.getenv("WORKERS", "1"))
ALLOWED_ORIGINS = os.getenv(
	"ALLOWED_ORIGINS", "http://frontend:5173"
).split(",")

# Resolve relative paths from BACKEND_ROOT
if not os.path.isabs(DOWNLOADS_DIR):
	DOWNLOADS_DIR = os.path.join(BACKEND_ROOT, DOWNLOADS_DIR)
if not os.path.isabs(TEMP_DIR):
	TEMP_DIR = os.path.join(BACKEND_ROOT, TEMP_DIR)

# Ensure directories exist at startup
os.makedirs(DOWNLOADS_DIR, exist_ok=True)
os.makedirs(TEMP_DIR, exist_ok=True)

# Maximum allowed upload file size in megabytes
MAX_FILE_SIZE: int = int(os.getenv("MAX_UPLOAD_SIZE_MB", "50"))

# Allowed file extensions (lowercase, without dots)
ALLOWED_EXTENSIONS = ("csv",)

# Path where uploaded files may be temporarily stored (project-relative)
TEMP_UPLOAD_DIR: str = TEMP_DIR

# Default encodings to try when reading CSVs
DEFAULT_ENCODING = ["utf-8", "latin-1"]


# Default cleaning rules used by `clean_dataframe` and the pipeline.
# Call `get_cleaning_rules()` to obtain the effective rules; callers may
# pass an `overrides` dict to change behavior at runtime.
DEFAULT_CLEANING_RULES: Dict[str, Any] = {
	"fill_missing_numeric": "median",
	"fill_missing_categorical": "mode",
	"drop_duplicates": True,
	"trim_strings": True,
	"drop_empty_columns": True,
	"convert_types": True,
}


def get_validated_config(user_config: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
	"""Merge `user_config` with defaults and validate allowed values.

	Raises ValueError on invalid values.
	"""
	cfg = dict(DEFAULT_CLEANING_RULES)
	if user_config:
		if not isinstance(user_config, dict):
			raise ValueError("user_config must be a dict")
		cfg.update(user_config)

	# Validate fill strategies
	num_allowed = {"median", "mean", "mode", "fixed"}
	cat_allowed = {"mode", "unknown", "none", "fixed"}

	fnum = str(cfg.get("fill_missing_numeric", "")).lower()
	if fnum not in num_allowed:
		raise ValueError(f"Invalid fill_missing_numeric: {fnum}. Allowed: {sorted(num_allowed)}")

	fcat = str(cfg.get("fill_missing_categorical", "")).lower()
	if fcat not in cat_allowed:
		raise ValueError(f"Invalid fill_missing_categorical: {fcat}. Allowed: {sorted(cat_allowed)}")

	# Boolean flags
	for bkey in ("drop_duplicates", "trim_strings", "drop_empty_columns", "convert_types"):
		val = cfg.get(bkey)
		if not isinstance(val, bool):
			# Allow truthy/falsey but enforce bool type for clarity
			if val in (0, 1):
				cfg[bkey] = bool(val)
			else:
				raise ValueError(f"Invalid value for {bkey}: {val}; expected boolean")

	return cfg


def get_cleaning_rules(overrides: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
	"""Return the effective cleaning rules.

	Merges defaults with environment JSON and `overrides`, then validates.

	Precedence (highest → lowest): `overrides` arg, `CLEANING_RULES_JSON`, defaults.
	"""
	rules = dict(DEFAULT_CLEANING_RULES)

	# Env var override (JSON string)
	env_val = os.getenv("CLEANING_RULES_JSON")
	if env_val:
		try:
			env_rules = json.loads(env_val)
			if isinstance(env_rules, dict):
				rules.update(env_rules)
		except Exception:
			# Ignore malformed JSON and keep defaults; callers may validate
			pass

	# Programmatic overrides
	if overrides:
		# Allow callers to pass either a dict or validated config
		if isinstance(overrides, dict):
			rules.update(overrides)
		else:
			# if someone passes a module or other type, skip
			pass

	# Validate and return
	return get_validated_config(rules)


# Schema configuration
# Schemas are stored in backend/schemas directory (not in app/)
SCHEMAS_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(__file__))), "schemas")
DEFAULT_SCHEMA_NAME = "default_schema.json"


def load_schema(schema_name: Optional[str] = None) -> Dict[str, Any]:
	"""Load a schema definition from the schemas directory.
	
	Args:
		schema_name: Name of the schema file (e.g., "default_schema.json").
		           If None, loads the default schema.
	
	Returns:
		Dictionary containing the schema definition
		
	Raises:
		FileNotFoundError: If schema file doesn't exist
		ValueError: If schema file is invalid JSON
	"""
	if schema_name is None:
		schema_name = DEFAULT_SCHEMA_NAME
	
	schema_path = os.path.join(SCHEMAS_DIR, schema_name)
	
	if not os.path.exists(schema_path):
		raise FileNotFoundError(f"Schema file not found: {schema_path}")
	
	try:
		with open(schema_path, 'r', encoding='utf-8') as f:
			schema_data = json.load(f)
		return schema_data
	except json.JSONDecodeError as e:
		raise ValueError(f"Invalid JSON in schema file {schema_name}: {e}")


def get_default_schema() -> Dict[str, Any]:
	"""Get the default schema configuration.
	
	Returns:
		Dictionary containing the default schema, or empty dict if not found
	"""
	try:
		return load_schema(DEFAULT_SCHEMA_NAME)
	except (FileNotFoundError, ValueError):
		# Return empty schema if default not found
		return {"schema": {"fields": {}}}


def list_available_schemas() -> list:
	"""List all available schema files in the schemas directory.
	
	Returns:
		List of schema filenames
	"""
	if not os.path.exists(SCHEMAS_DIR):
		return []
	
	return [f for f in os.listdir(SCHEMAS_DIR) if f.endswith('.json')]
