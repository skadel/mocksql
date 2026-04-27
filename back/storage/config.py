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


def get_bq_test_dataset() -> str:
    cfg = load_config()
    return cfg.get("test_dataset") or os.getenv("BQ_TEST_DATASET", "test_dataset")


def get_langchain_api_key() -> str | None:
    cfg = load_config()
    return cfg.get("langchain_api_key") or os.getenv("LANGCHAIN_API_KEY")


def get_langchain_tracing() -> bool:
    cfg = load_config()
    if "langchain_tracing" in cfg:
        return bool(cfg["langchain_tracing"])
    # fall back to env var if config not present
    return os.getenv("LANGCHAIN_TRACING_V2", "false").lower() == "true"


def is_initialized() -> bool:
    """Return True if mocksql.yml exists in the current working directory."""
    return (Path(os.getcwd()) / "mocksql.yml").exists()


def get_preprocessor_fn() -> str | None:
    return load_config().get("preprocessor_fn")


def load_preprocessor_fn(fn_ref: str, config_dir: Path):
    import importlib
    import sys

    if ":" not in fn_ref:
        raise ValueError(
            f"preprocessor_fn must be in 'module:function' format, got: {fn_ref!r}"
        )
    module_name, func_name = fn_ref.split(":", 1)

    config_dir_str = str(config_dir.resolve())
    if config_dir_str not in sys.path:
        sys.path.insert(0, config_dir_str)

    module = importlib.import_module(module_name)
    fn = getattr(module, func_name, None)
    if fn is None:
        raise AttributeError(
            f"Function '{func_name}' not found in module '{module_name}'"
        )
    return fn
