from typing import Any, Dict, List

import yaml


DEFAULT_STEP_CONFIGS = {
    "impute": {"strategy": "median", "apply_to": "numeric"},
    "deduplicate": {"subset": "all"},
    "trim_whitespace": {"apply_to": "all"},
    "remove_outliers": {"method": "iqr", "threshold": 1.5},
    "type_inference": {"enabled": True},
    "drop_empty_columns": {"threshold": 0.9},
}


def parse_pipeline_yaml(yaml_string: str) -> List[Dict[str, Any]]:
    """Parse YAML pipeline config and fill defaults per step."""
    try:
        config = yaml.safe_load(yaml_string)
    except yaml.YAMLError as exc:
        raise ValueError(f"Invalid YAML: {exc}")

    if not isinstance(config, dict):
        raise ValueError("YAML must define an object with a 'pipeline.steps' key")

    if "pipeline" not in config or "steps" not in config["pipeline"]:
        raise ValueError("YAML must contain a 'pipeline.steps' key")

    raw_steps = config["pipeline"]["steps"]
    if not isinstance(raw_steps, list):
        raise ValueError("pipeline.steps must be a list")

    steps: List[Dict[str, Any]] = []
    for raw_step in raw_steps:
        if not isinstance(raw_step, dict) or "step" not in raw_step:
            raise ValueError(f"Each step must have a 'step' key: {raw_step}")
        step_name = raw_step["step"]
        if step_name not in DEFAULT_STEP_CONFIGS:
            raise ValueError(f"Unknown step: {step_name}")
        merged = {**DEFAULT_STEP_CONFIGS[step_name], **raw_step}
        steps.append(merged)

    return steps


def pipeline_steps_to_config(steps: List[Dict]) -> Dict[str, Any]:
    """Convert parsed pipeline steps to existing cleaning config format."""
    config: Dict[str, Any] = {}
    for step in steps:
        name = step["step"]
        if name == "impute":
            config["missing_strategy"] = step["strategy"]
        elif name == "remove_outliers":
            config["remove_outliers"] = True
            config["outlier_method"] = step["method"]
            config["outlier_threshold"] = step["threshold"]
        elif name == "deduplicate":
            config["drop_duplicates"] = True
        elif name == "drop_empty_columns":
            config["empty_col_threshold"] = step["threshold"]
        elif name == "type_inference":
            config["fix_types"] = step["enabled"]
    return config
