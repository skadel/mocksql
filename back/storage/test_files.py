"""Lecture / écriture du fichier de test avec split définition (commité) / cache (gitignoré).

Le fichier `.mocksql/tests/{model}.json` est **commité** et doit rester lisible et
éditable à la main : il ne porte que la *définition* du test (sql, used_columns,
données, assertions, verdict). Tout ce qui est dérivable du SQL ou purement runtime
(`optimized_sql`, `query_decomposed`, `results_json`, `status`, raisonnement LLM…)
part dans un cache **gitignoré** `.mocksql/cache/{model}.json`.

La fusion est transparente à la lecture : `read_test_doc` recompose un dict
strictement identique à l'historique (mêmes clés, mêmes types — y compris les
champs JSON-encodés-en-string), si bien qu'aucun consommateur en aval n'a à changer.
`used_columns` est stocké en JSON imbriqué lisible sur le disque mais ré-encodé en
`list[str]` en mémoire (back-compat avec tout le code qui fait `json.loads`).
"""

import json
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

# Champs dérivables-du-SQL ou runtime, sortis du fichier commité vers le cache.
_CACHE_TOP_KEYS: Tuple[str, ...] = ("optimized_sql", "query_decomposed", "last_error")
_CACHE_CASE_KEYS: Tuple[str, ...] = (
    "unit_test_build_reasoning",
    "status",
    "results_json",
    "reason_type",
)


def cache_path_for(tests_path: Path) -> Optional[Path]:
    """`.../tests/<rel>.json` → `.../cache/<rel>.json`, ou None si pas de segment `tests`."""
    parts = list(tests_path.parts)
    for i in range(len(parts) - 1, -1, -1):
        if parts[i] == "tests":
            parts[i] = "cache"
            return Path(*parts)
    return None


def _destringify_used_columns(uc: Any) -> Any:
    """`list[str-json]` (mémoire) → `list[dict]` (disque, lisible). No-op si déjà dict."""
    if not isinstance(uc, list):
        return uc
    out: List[Any] = []
    for item in uc:
        if isinstance(item, str):
            try:
                out.append(json.loads(item))
                continue
            except Exception:
                pass
        out.append(item)
    return out


def _restringify_used_columns(uc: Any) -> Any:
    """`list[dict]` (disque) → `list[str-json]` (mémoire, back-compat). No-op si déjà str."""
    if not isinstance(uc, list):
        return uc
    return [
        json.dumps(item, ensure_ascii=False) if isinstance(item, dict) else item
        for item in uc
    ]


def split_doc(doc: Dict[str, Any]) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """Sépare un doc (forme mémoire) en (définition commitée, cache gitignoré)."""
    definition = dict(doc)
    cache: Dict[str, Any] = {}
    for k in _CACHE_TOP_KEYS:
        if k in definition:
            cache[k] = definition.pop(k)
    if "used_columns" in definition:
        definition["used_columns"] = _destringify_used_columns(
            definition["used_columns"]
        )

    case_cache: Dict[str, Dict[str, Any]] = {}
    clean_cases: List[Dict[str, Any]] = []
    for i, case in enumerate(definition.get("test_cases") or []):
        c = dict(case)
        popped = {k: c.pop(k) for k in _CACHE_CASE_KEYS if k in c}
        if popped:
            case_cache[str(c.get("test_index", i))] = popped
        clean_cases.append(c)
    if definition.get("test_cases") is not None:
        definition["test_cases"] = clean_cases
    if case_cache:
        cache["test_cases"] = case_cache
    return definition, cache


def merge_doc(
    definition: Dict[str, Any], cache: Optional[Dict[str, Any]]
) -> Dict[str, Any]:
    """Recompose le dict mémoire à partir de la définition et du cache (peut être None)."""
    doc = dict(definition)
    if "used_columns" in doc:
        doc["used_columns"] = _restringify_used_columns(doc["used_columns"])
    cache = cache or {}
    for k in _CACHE_TOP_KEYS:
        if k in cache:
            doc[k] = cache[k]
    case_cache = cache.get("test_cases") or {}
    cases = doc.get("test_cases")
    if isinstance(cases, list):
        merged: List[Dict[str, Any]] = []
        for i, case in enumerate(cases):
            c = dict(case)
            extra = case_cache.get(str(c.get("test_index", i)))
            if extra:
                c.update(extra)
            merged.append(c)
        doc["test_cases"] = merged
    return doc


def read_test_doc(path: Path) -> Optional[Dict[str, Any]]:
    """Lit la définition + le cache sidecar (si présent) et renvoie le dict fusionné."""
    try:
        definition = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    cache: Optional[Dict[str, Any]] = None
    cp = cache_path_for(path)
    if cp and cp.exists():
        try:
            cache = json.loads(cp.read_text(encoding="utf-8"))
        except Exception:
            cache = None
    return merge_doc(definition, cache)


def write_test_doc(path: Path, doc: Dict[str, Any]) -> None:
    """Écrit la définition (commitée) et le cache gitignoré côte à côte.

    Le cache est supprimé s'il devient vide, pour ne pas laisser de fichier mort.
    """
    definition, cache = split_doc(doc)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(definition, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    cp = cache_path_for(path)
    if cp is None:
        return
    if cache:
        cp.parent.mkdir(parents=True, exist_ok=True)
        cp.write_text(
            json.dumps(cache, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
    elif cp.exists():
        cp.unlink()
