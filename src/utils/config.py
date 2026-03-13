"""Simple configuration loading for quarantine-sdp pipeline."""

import yaml
from pathlib import Path
from src.utils.constants import DEFAULT_QUARANTINE_HOURS, DEFAULT_REQUIRED_COLUMNS


def load_config(config_path: str = "config.yaml") -> dict:
    """Load configuration from YAML file.

    Args:
        config_path: Path to config.yaml (defaults to current directory)

    Returns:
        dict with config values
    """
    # Try to find config.yaml
    config_file = Path(config_path)
    if not config_file.exists():
        # Search up to 3 parent directories
        for parent in [Path.cwd()] + list(Path.cwd().parents[:3]):
            config_file = parent / "config.yaml"
            if config_file.exists():
                break

    with open(config_file, 'r') as file:
        config = yaml.safe_load(file)

    # Add defaults for missing keys
    config.setdefault('Quarantine_Hours', DEFAULT_QUARANTINE_HOURS)
    config.setdefault('Required_Columns', DEFAULT_REQUIRED_COLUMNS)

    return config
