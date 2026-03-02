import yaml
from pathlib import Path
from typing import Any, Dict

def load_config(config_path: str = "config.yaml") -> Dict[str, Any]:
    """Load the YAML configuration file."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)
