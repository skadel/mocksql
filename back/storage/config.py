import os
from functools import lru_cache
from pathlib import Path

import yaml


def _base_dir() -> Path:
    base = os.getenv("MOCKSQL_BASE_DIR")
    if base:
        return Path(base).resolve()
    return Path(os.getcwd())


def get_mocksql_dir() -> Path:
    return _base_dir() / ".mocksql"


@lru_cache(maxsize=1)
def load_config() -> dict:
    path = _base_dir() / "mocksql.yml"
    if not path.exists():
        return {}
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f) or {}


def get_models_path() -> Path:
    cfg = load_config()
    raw = cfg.get("models_path", "./models")
    return (_base_dir() / raw).resolve()


_GITIGNORE_CONTENT = "data/\n"


def ensure_mocksql_dir(mocksql_dir: Path) -> None:
    mocksql_dir.mkdir(parents=True, exist_ok=True)
    gitignore = mocksql_dir / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text(_GITIGNORE_CONTENT, encoding="utf-8")


def get_duckdb_path() -> str:
    # env var override kept for tests (:memory:) and CI
    env_override = os.getenv("DUCKDB_PATH")
    if env_override:
        return env_override
    return str(get_mocksql_dir() / "data" / "mocksql.duckdb")


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
    return (_base_dir() / "mocksql.yml").exists()


def get_llm_model() -> str:
    cfg = load_config()
    return cfg.get("llm", {}).get("model") or os.getenv(
        "DEFAULT_MODEL_NAME", "gemini-2.5-flash"
    )


def get_llm_streaming() -> bool:
    cfg = load_config()
    llm_cfg = cfg.get("llm", {})
    if "streaming" in llm_cfg:
        return bool(llm_cfg["streaming"])
    return os.getenv("LLM_STREAMING", "true").lower() == "true"


def get_llm_location() -> str | None:
    cfg = load_config()
    return cfg.get("llm", {}).get("location") or os.getenv("LLM_LOCATION")


def get_llm_thinking_level() -> str | None:
    cfg = load_config()
    return (
        cfg.get("llm", {}).get("thinking_level")
        or os.getenv("LLM_THINKING_LEVEL")
        or None
    )


def get_llm_thinking_budget() -> int | None:
    cfg = load_config()
    # Ne pas utiliser `or` : thinking_budget=0 (désactivation du thinking Gemini)
    # est falsy et serait écrasé par le fallback env. Distinguer "absent" de 0.
    val = cfg.get("llm", {}).get("thinking_budget")
    if val is None:
        val = os.getenv("LLM_THINKING_BUDGET")
    if val is None:
        return None
    try:
        return int(val)
    except (ValueError, TypeError):
        return None


def is_native_thinking_active() -> bool:
    """Indique si le modèle raisonne nativement (canal thinking séparé).

    Ne pas confondre avec « thinking_budget est défini » : quand aucun budget
    n'est passé, c'est le **défaut serveur du modèle** qui s'applique (et non
    « désactivé »). Sur Gemini 2.5, le thinking est ON par défaut sauf sur
    `flash-lite`. On combine donc le réglage explicite et le défaut du modèle.

    Sert à décider si le champ `unit_test_build_reasoning` doit porter un vrai
    chain-of-thought (thinking inactif → seul raisonnement disponible) ou juste
    une justification courte (thinking actif → le raisonnement se fait en amont).
    """
    budget = get_llm_thinking_budget()
    if budget is not None:
        return budget > 0
    if get_llm_thinking_level() is not None:
        return True

    model = get_llm_model().lower()
    # `flash-lite` contient `flash` — le tester en premier.
    if "lite" in model:
        return False
    if "gemini-2.5-flash" in model or "gemini-2.5-pro" in model or "gemini-3" in model:
        return True
    # Modèle inconnu : on suppose le thinking inactif → on garde le CoT in-schema
    # (sûr : pas de perte de qualité, juste un champ un peu plus long).
    return False


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
