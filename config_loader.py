import os
from typing import Any

import yaml

# ============================================================
# LOAD YAML CONFIG
# ============================================================

def replace_none_with_empty_string(d: Any) -> Any:
    """Recursively replace all None values in a dict/list with empty strings.

    :param d: Dictionary, list, or value to process
    :type d: any
    :return: Dictionary, list, or value with None replaced by ''
    :rtype: any
    """
    if isinstance(d, dict):
        return {k: replace_none_with_empty_string(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [replace_none_with_empty_string(v) for v in d]
    elif d is None:
        return ""
    else:
        return d


def resolve_env_vars(d: Any) -> Any:
    """Recursively replace environment variable placeholders (e.g., ${VAR}) in strings.

    :param d: Dictionary, list, or value to process
    :type d: any
    :return: Dictionary, list, or value with environment variables expanded
    :rtype: any
    """
    if isinstance(d, dict):
        return {k: resolve_env_vars(v) for k, v in d.items()}
    elif isinstance(d, list):
        return [resolve_env_vars(v) for v in d]
    elif isinstance(d, str):
        return os.path.expandvars(d)
    else:
        return d


def load_config(alias: str) -> dict:
    """Load a YAML configuration file, replace None with empty strings, resolve environment variables,
    and flatten 'source' + 'config' sections for compatibility with App.

    :param alias: Configuration alias corresponding to config/{alias}.yml
    :type alias: str
    :return: Flattened configuration dictionary
    :rtype: dict
    :raises FileNotFoundError: if the YAML config file does not exist
    """
    config_path = os.path.join("config", f"{alias}.yml")

    if not os.path.isfile(config_path):
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with open(config_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)

    # Replace None values with empty strings
    config = replace_none_with_empty_string(config)
    # Resolve environment variables
    config = resolve_env_vars(config)

    # Flatten 'source' and 'config' sections for App compatibility
    flat_config = {}
    flat_config.update(config.get("source", {}))
    flat_config.update(config.get("config", {}))

    # Ensure ADDITIONAL_EXCLUDED_COLLECTIONS exists and is a lowercase set
    flat_config["ADDITIONAL_EXCLUDED_COLLECTIONS"] = set(
        c.lower() for c in flat_config.get("ADDITIONAL_EXCLUDED_COLLECTIONS", [])
    )
    print('config', flat_config)
    return flat_config