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


# profile.json contient des valeurs brutes de l'entrepôt (PII) → jamais commité.
# data/ = base DuckDB locale. Le réplay CI ne dépend d'aucun des deux.
_GITIGNORE_CONTENT = "data/\nprofile.json\n"


def ensure_mocksql_dir(mocksql_dir: Path) -> None:
    mocksql_dir.mkdir(parents=True, exist_ok=True)
    gitignore = mocksql_dir / ".gitignore"
    if not gitignore.exists():
        gitignore.write_text(_GITIGNORE_CONTENT, encoding="utf-8")
        return
    # Projet existant : garantir que chaque entrée requise est présente (ajout
    # idempotent) — notamment profile.json sur les .mocksql créés avant le split.
    existing = gitignore.read_text(encoding="utf-8")
    lines = {ln.strip() for ln in existing.splitlines()}
    missing = [e for e in _GITIGNORE_CONTENT.split() if e not in lines]
    if missing:
        sep = "" if existing.endswith("\n") or not existing else "\n"
        gitignore.write_text(
            existing + sep + "\n".join(missing) + "\n", encoding="utf-8"
        )


def get_profile_cache_path() -> str:
    """Chemin du cache profil (PII) — gitignoré, séparé du schema_cache commité.

    Surchargeable via `profile_cache` dans mocksql.yml (parité avec `schema_cache`),
    sinon `.mocksql/profile.json`. L'env `PROFILE_CACHE_PATH` reste prioritaire (géré
    en amont dans env_variables). Résolu sous MOCKSQL_BASE_DIR comme get_models_path.
    """
    raw = load_config().get("profile_cache", ".mocksql/profile.json")
    p = Path(raw)
    if not p.is_absolute():
        p = _base_dir() / p
    return str(p)


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


def get_dbt_project():
    """Retourne un `DbtProject` si un bloc `dbt:` est configuré dans mocksql.yml, sinon None.

    Config attendue :
        dbt:
          project_dir: ../warehouse   # relatif au dossier de mocksql.yml
          target_path: target         # optionnel (défaut: target)

    Quand un projet dbt est configuré, MockSQL lit le SQL **compilé** (refs résolus,
    macros rendues) et infère les schémas amont depuis le manifest — sans entrepôt.
    """
    cfg = load_config().get("dbt")
    if not cfg or not cfg.get("project_dir"):
        return None
    from storage.dbt_manifest import DbtProject

    project_dir = (_base_dir() / cfg["project_dir"]).resolve()
    return DbtProject(project_dir, cfg.get("target_path", "target"))


def get_duckdb_extensions() -> list[str]:
    """Extensions DuckDB à charger sur chaque connexion (ex: spatial, json).

    Déclarées dans mocksql.yml :

        duckdb:
          extensions:
            - spatial
    """
    exts = load_config().get("duckdb", {}).get("extensions", [])
    if isinstance(exts, str):
        exts = [exts]
    return [str(e).strip() for e in exts if str(e).strip()]


def apply_duckdb_extensions(con) -> None:
    """Installe et charge les extensions configurées sur une connexion DuckDB.

    Idempotent (INSTALL/LOAD sont sûrs à rejouer). Une extension qui échoue
    (réseau absent au premier INSTALL, nom inconnu) est journalisée en warning
    et n'interrompt pas l'ouverture de la connexion — l'erreur de requête en
    aval restera explicite.
    """
    import logging

    logger = logging.getLogger(__name__)
    for ext in get_duckdb_extensions():
        try:
            con.execute(f"INSTALL {ext}")
            con.execute(f"LOAD {ext}")
        except Exception as e:  # pragma: no cover - dépend de l'env réseau
            logger.warning(
                "Extension DuckDB '%s' non chargée: %r "
                "(les requêtes qui en dépendent échoueront)",
                ext,
                e,
            )


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
