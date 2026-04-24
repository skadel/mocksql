import os
from functools import lru_cache
from pathlib import Path

import yaml


@lru_cache(maxsize=1)
def load_config() -> dict:
    path = Path(os.getcwd()) / "mocksql.yml"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_models_path() -> Path:
    cfg = load_config()
    raw = cfg.get("models_path", "./models")
    return (Path(os.getcwd()) / raw).resolve()


def get_mocksql_dir() -> Path:
    return (Path(os.getcwd()) / ".mocksql").resolve()
