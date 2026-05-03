from __future__ import annotations

import sqlglot
import sqlglot.expressions as exp


_WEIGHTS: dict[str, float] = {
    "window_functions": 3.0,
    "regex": 2.0,
    "case_when": 1.0,
    "subqueries": 1.0,
    "joins": 0.5,
    "ctes": 0.5,
}

_COMPLEXITY_CAP = 20.0
_COMPLEXITY_MAX_SCORE = 60.0
_RECENCY_CAP = 10
_RECENCY_MAX_SCORE = 40.0


def compute_complexity_score(sql: str, dialect: str = "bigquery") -> dict:
    """Parse SQL with sqlglot and return a weighted complexity breakdown.

    Returns a dict with keys:
      - total: raw weighted score (float)
      - breakdown: dict of signal -> count
    """
    try:
        tree = sqlglot.parse_one(
            sql,
            dialect=dialect,
            error_level=sqlglot.ErrorLevel.IGNORE,
        )
    except Exception:
        return {"total": 0.0, "breakdown": {}}

    if tree is None:
        return {"total": 0.0, "breakdown": {}}

    breakdown: dict[str, int] = {}

    windows = list(tree.find_all(exp.Window))
    if windows:
        breakdown["window_functions"] = len(windows)

    regex_nodes = list(
        tree.find_all(
            exp.RegexpLike, exp.RegexpExtract, exp.RegexpReplace, exp.RegexpILike
        )
    )
    if regex_nodes:
        breakdown["regex"] = len(regex_nodes)

    cases = list(tree.find_all(exp.Case))
    if cases:
        breakdown["case_when"] = len(cases)

    subqueries = list(tree.find_all(exp.Subquery))
    if subqueries:
        breakdown["subqueries"] = len(subqueries)

    joins = list(tree.find_all(exp.Join))
    if joins:
        breakdown["joins"] = len(joins)

    withs = list(tree.find_all(exp.With))
    cte_count = sum(len(w.expressions) for w in withs)
    if cte_count:
        breakdown["ctes"] = cte_count

    total = sum(breakdown.get(k, 0) * w for k, w in _WEIGHTS.items())

    return {"total": round(total, 1), "breakdown": breakdown}


def compute_priority_score(complexity_total: float, recent_commits: int) -> float:
    """Combine complexity and git recency into a 0–100 priority score."""
    complexity_norm = (
        min(complexity_total / _COMPLEXITY_CAP, 1.0) * _COMPLEXITY_MAX_SCORE
    )
    recency_norm = min(recent_commits / _RECENCY_CAP, 1.0) * _RECENCY_MAX_SCORE
    return round(complexity_norm + recency_norm, 1)
