"""Estimation grossière de la durée d'une génération de tests (en minutes).

Volontairement **pas** du ML : le « modèle » est le fichier de durées observées
(`gen_timings.jsonl`), et l'estimation est une **moyenne** des générations passées
de complexité comparable. Quatre features grosse-maille décrivent une requête :

  - ``sql_len``       : longueur du SQL (caractères)
  - ``n_tables``      : nombre de tables sources
  - ``n_used_cols``   : nombre de colonnes réellement utilisées
  - ``n_constraints`` : nombre de prédicats (proxy AST rapide, <1s — PAS
                        constraint_simplifier qui coûte ~10s)

Deux usages :
  - **collecte** : ``log_timing(...)`` appendé en fin de ``run_generate``
  - **service**  : ``estimate_minutes(...)`` appelé AVANT la génération pour
                   afficher « le travail peut prendre N minutes ».
"""

from __future__ import annotations

import json
import math
import statistics
from pathlib import Path
from typing import Any

import sqlglot
from sqlglot import exp

from utils.sql_code import extract_real_table_refs

# Dataset des durées observées + défaut quand on n'a encore rien collecté.
_DATA_DIR = Path(__file__).resolve().parent / "data"
_TIMINGS_PATH = _DATA_DIR / "gen_timings.jsonl"
_DEFAULT_MINUTES = 3

# Prédicats comptés comme « contraintes » (proxy grosse-maille du nombre réel).
_PREDICATE_TYPES: tuple[type, ...] = (
    exp.EQ,
    exp.NEQ,
    exp.GT,
    exp.GTE,
    exp.LT,
    exp.LTE,
    exp.In,
    exp.Like,
    exp.ILike,
    exp.Is,
    exp.Between,
)


def count_constraints(sql: str, dialect: str = "bigquery") -> int:
    """Proxy rapide du nombre de contraintes : compte les prédicats de l'AST.

    Approxime ``constraint_simplifier`` (~10s) en <1s — suffisant pour une
    estimation grosse-maille. Retourne 0 si le SQL ne parse pas.
    """
    try:
        ast = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        return 0
    return sum(1 for _ in ast.find_all(*_PREDICATE_TYPES))


def _count_used_columns(used_columns: list[Any] | None) -> int:
    """Somme des colonnes utilisées à travers les entrées (str JSON ou dict)."""
    total = 0
    for uc in used_columns or []:
        try:
            entry = json.loads(uc) if isinstance(uc, str) else uc
        except Exception:
            continue
        if isinstance(entry, dict):
            total += len(entry.get("used_columns") or [])
    return total


def extract_features(
    sql: str,
    used_columns: list[Any] | None = None,
    dialect: str = "bigquery",
) -> dict[str, int]:
    """Calcule les 4 features grosse-maille d'une requête.

    ``used_columns`` est optionnel : s'il est absent (estimation pré-génération
    où on ne l'a pas encore calculé), ``n_used_cols`` vaut 0.
    """
    try:
        n_tables = len(extract_real_table_refs(sql, dialect))
    except Exception:
        n_tables = 0
    return {
        "sql_len": len(sql or ""),
        "n_tables": n_tables,
        "n_used_cols": _count_used_columns(used_columns),
        "n_constraints": count_constraints(sql, dialect),
    }


def _bucket(features: dict[str, int]) -> tuple[int, int]:
    """Classe une requête dans un bucket grossier (n_tables, tranche de taille).

    On regroupe par nombre de tables (plafonné à 3+) et par tranche de longueur
    de SQL — deux requêtes du même bucket sont supposées prendre un temps
    comparable. Sert à moyenner uniquement des générations « semblables ».
    """
    n_tables = min(features.get("n_tables", 0), 3)
    sql_len = features.get("sql_len", 0)
    if sql_len < 500:
        size = 0
    elif sql_len < 1500:
        size = 1
    else:
        size = 2
    return (n_tables, size)


def _load_samples(path: Path = _TIMINGS_PATH) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    samples: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            samples.append(json.loads(line))
        except Exception:
            continue
    return samples


def log_timing(
    features: dict[str, int],
    elapsed_sec: float,
    path: Path = _TIMINGS_PATH,
) -> None:
    """Appende une observation ``(features, durée)`` au dataset (JSONL).

    Best-effort : une erreur d'écriture ne doit jamais faire échouer une
    génération réussie.
    """
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        row = {**features, "elapsed_sec": round(float(elapsed_sec), 2)}
        with open(path, "a", encoding="utf-8") as f:
            f.write(json.dumps(row) + "\n")
    except Exception:
        pass


def estimate_minutes(
    features: dict[str, int],
    path: Path = _TIMINGS_PATH,
) -> int:
    """Estime la durée (minutes, arrondie au supérieur, plancher 1).

    Moyenne des durées observées dans le même bucket ; à défaut, moyenne
    globale ; à défaut (dataset vide), ``_DEFAULT_MINUTES``.
    """
    samples = _load_samples(path)
    if not samples:
        return _DEFAULT_MINUTES

    target = _bucket(features)
    same_bucket = [
        s["elapsed_sec"] for s in samples if "elapsed_sec" in s and _bucket(s) == target
    ]
    pool = same_bucket or [s["elapsed_sec"] for s in samples if "elapsed_sec" in s]
    if not pool:
        return _DEFAULT_MINUTES

    avg_sec = statistics.mean(pool)
    return max(1, math.ceil(avg_sec / 60))
