import logging
import yaml
from datetime import datetime
from pathlib import Path
from typing import Any, Dict


def setup_logging(log_dir: str = "logs") -> None:
    """Configure logging to console and a timestamped file."""
    Path(log_dir).mkdir(exist_ok=True)
    log_file = Path(log_dir) / f"{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

    fmt = "%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"

    logging.basicConfig(
        level=logging.INFO,
        format=fmt,
        datefmt=datefmt,
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler(log_file, encoding="utf-8"),
        ],
    )
    logging.info(f"Logs written to: {log_file}")


_VALID_INDICES = {"NDVI", "NDWI", "SAVI", "NBR", "NDBI", "NDRE1", "NDRE2", "NDRE3"}


def _validate_config(cfg: Dict[str, Any]) -> None:
    """Validate required config keys and raise ValueError with explicit messages."""
    errors = []

    for key, expected_type in [
        ("stac_api_url", str),
        ("collection", str),
        ("indices", list),
    ]:
        if key not in cfg:
            errors.append(f"Missing key: '{key}'")
        elif not isinstance(cfg[key], expected_type):
            errors.append(f"'{key}' must be {expected_type.__name__}, got {type(cfg[key]).__name__}")

    cfg.setdefault("save_zarr", False)
    if not isinstance(cfg["save_zarr"], bool):
        errors.append(f"'save_zarr' must be bool, got {type(cfg['save_zarr']).__name__}")

    cfg.setdefault("debug_cloud", False)
    if not isinstance(cfg["debug_cloud"], bool):
        errors.append(f"'debug_cloud' must be bool, got {type(cfg['debug_cloud']).__name__}")

    cfg.setdefault("batch_size_deg", 0.5)
    if not isinstance(cfg["batch_size_deg"], (int, float)) or cfg["batch_size_deg"] <= 0:
        errors.append("'batch_size_deg' must be a number > 0")

    if "indices" in cfg and isinstance(cfg["indices"], list):
        if not cfg["indices"]:
            errors.append("'indices' cannot be empty")
        else:
            invalid = set(cfg["indices"]) - _VALID_INDICES
            if invalid:
                errors.append(f"Unknown indices: {invalid}. Accepted values: {_VALID_INDICES}")

    if "dask" not in cfg:
        errors.append("Missing key: 'dask'")
    else:
        for sub_key in ("n_workers", "threads_per_worker"):
            if sub_key not in cfg["dask"]:
                errors.append(f"Missing key: 'dask.{sub_key}'")
            elif not isinstance(cfg["dask"][sub_key], int) or cfg["dask"][sub_key] <= 0:
                errors.append(f"'dask.{sub_key}' must be an integer > 0")

    if errors:
        raise ValueError("Invalid configuration:\n" + "\n".join(f"  - {e}" for e in errors))


def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load the YAML configuration file."""
    with open(config_path, "r") as f:
        cfg = yaml.safe_load(f)
    _validate_config(cfg)
    return cfg
