"""Contrat ``expect`` — lignes attendues concrètes, confirmées par un humain.

Implémente la spec ``docs/spec-validation-humaine.md`` (Phase 0, shadow) : le contrat
d'un test devient un snapshot des lignes observées (``expect.columns`` /
``expect.rows`` / ``expect.ordered``), destiné à être confirmé par l'utilisateur
(approval testing). En Phase 0 il est **dual-writé** à côté des assertions existantes
et comparé en parallèle au replay (``mocksql test``) pour mesurer l'accord des deux
approches — sans changer le verdict ni le code de sortie.

Zéro dépendance LLM/langchain : ce module est importé par le replay CI
(``cli/test_runner.py``) qui ne doit pas payer le chargement du stack LLM — même
posture que ``assertion_eval``.
"""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import sqlglot
from sqlglot import exp

# Nombre max de lignes de diff exposées par côté (lisibilité du rapport de replay).
DIFF_CAP = 5

# Sentinelle de valeur pour une colonne absente d'une ligne (≠ NULL : une colonne
# manquante dans la sortie est un mismatch de schéma, pas une valeur nulle).
_ABSENT = ("__absent__",)


# ── Normalisation des lignes ──────────────────────────────────────────────────


def rows_from_results_json(results_json: Any) -> Optional[List[Dict[str, Any]]]:
    """Parse ``results_json`` (JSON records string) en liste de dicts, ou None."""
    if isinstance(results_json, list):
        rows = results_json
    elif isinstance(results_json, str):
        try:
            rows = json.loads(results_json)
        except Exception:
            return None
    else:
        return None
    if not isinstance(rows, list):
        return None
    return [r for r in rows if isinstance(r, dict)]


def rows_from_df(df) -> List[Dict[str, Any]]:
    """DataFrame → lignes normalisées par LE MÊME chemin pandas que ``results_json``
    (``to_json(orient="records", date_format="iso", date_unit="s")``, cf.
    ``examples_executor.format_result``). Passer les deux côtés de la comparaison par
    la même sérialisation garantit une normalisation identique (dates ISO, NaN→null,
    Decimal→float) — jamais de comparaison DataFrame brut vs JSON stocké.
    """
    payload = df.to_json(orient="records", date_format="iso", date_unit="s")
    rows = json.loads(payload)
    return rows if isinstance(rows, list) else []


def _canon_value(value: Any) -> Tuple[Any, ...]:
    """Forme canonique d'une valeur pour la comparaison.

    - bool AVANT numérique (bool est un sous-type d'int en Python) ;
    - int/float unifiés en 12 chiffres significatifs (10 vs 10.0, bruit float) ;
    - dict/list (colonnes JSON) canonisés par sérialisation triée.
    """
    if value is None:
        return ("z",)
    if isinstance(value, bool):
        return ("b", value)
    if isinstance(value, (int, float)):
        return ("n", f"{float(value):.12g}")
    if isinstance(value, (dict, list)):
        return ("j", json.dumps(value, sort_keys=True, ensure_ascii=False, default=str))
    return ("s", str(value))


def _project_row(row: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
    """Restreint une ligne aux colonnes du contrat, insensible à la casse des clés."""
    lowered = {str(k).lower(): v for k, v in row.items()}
    return {c: lowered.get(c.lower(), _ABSENT) for c in columns}


def _canon_row(row: Dict[str, Any], columns: List[str]) -> Tuple[Any, ...]:
    projected = _project_row(row, columns)
    return tuple(_canon_value(projected[c]) for c in columns)


def _display_row(row: Dict[str, Any], columns: List[str]) -> Dict[str, Any]:
    """Ligne projetée pour l'affichage du diff (colonne absente → "<absente>")."""
    projected = _project_row(row, columns)
    return {
        c: ("<absente>" if projected[c] is _ABSENT else projected[c]) for c in columns
    }


# ── Construction du contrat ───────────────────────────────────────────────────


def detect_ordered(sql: str, dialect: Optional[str] = None) -> bool:
    """True si le SQL se termine par un ORDER BY top-level (déterminisme AST, pas LLM).

    Best-effort : premier dialecte qui parse parmi (dialect, générique, bigquery,
    snowflake) ; aucun parse → False (multiset, le défaut sûr).
    """
    if not (sql or "").strip():
        return False
    tried: List[Optional[str]] = []
    for d in (dialect, None, "bigquery", "snowflake"):
        if d in tried:
            continue
        tried.append(d)
        try:
            tree = sqlglot.parse_one(sql, read=d)
        except Exception:
            continue
        if tree is None:
            continue
        try:
            return tree.args.get("order") is not None
        except Exception:
            return False
    return False


def _assertion_columns(
    assertions: Optional[List[Dict[str, Any]]], available: List[str]
) -> List[str]:
    """Colonnes du résultat référencées par les assertions existantes (spec §5 :
    « restreint aux colonnes des assertions quand elles sont identifiables »).

    Parse les fragments ``expected_condition`` / ``scope`` (sqlglot, dialecte duckdb —
    celui des assertions), collecte les ``exp.Column`` et les rapproche des colonnes
    du résultat (insensible à la casse, casse du résultat conservée). Intersection
    vide ou parse raté → [] (l'appelant retombe sur toutes les colonnes).
    """
    if not assertions:
        return []
    by_lower = {c.lower(): c for c in available}
    found: List[str] = []
    for a in assertions:
        for fragment in (a.get("expected_condition"), a.get("scope")):
            if not fragment or not str(fragment).strip():
                continue
            try:
                tree = sqlglot.parse_one(str(fragment), read="duckdb")
            except Exception:
                continue
            if tree is None:
                continue
            for col in tree.find_all(exp.Column):
                match = by_lower.get(col.name.lower())
                if match and match not in found:
                    found.append(match)
    return found


def _resolve_columns(requested: List[str], available: List[str]) -> List[str]:
    """Restreint ``requested`` aux colonnes réellement présentes (casse du résultat
    conservée, rapprochement insensible à la casse), en préservant l'ordre demandé.
    Aucune correspondance → [] (l'appelant retombe sur toutes les colonnes)."""
    by_lower = {c.lower(): c for c in available}
    out: List[str] = []
    for c in requested:
        match = by_lower.get(str(c).lower())
        if match and match not in out:
            out.append(match)
    return out


def build_expect(
    results_json: Any,
    assertions: Optional[List[Dict[str, Any]]],
    sql: str,
    dialect: Optional[str] = None,
    columns: Optional[List[str]] = None,
) -> Optional[Dict[str, Any]]:
    """Construit le contrat ``expect`` depuis la sortie observée.

    ``columns`` : quand fourni explicitement (colonnes porteuses choisies par le
    ``coherence_check``, spec §5), on s'y restreint ; sinon on retombe sur les colonnes
    des assertions quand identifiables, sinon toutes les colonnes du résultat.
    ``ordered`` : True ssi ORDER BY top-level dans le SQL. ``rows`` vide est un contrat
    VALIDE (sortie vide attendue) ; ``results_json`` illisible → None (cas non
    exprimable en lignes).
    """
    rows = rows_from_results_json(results_json)
    if rows is None:
        return None
    available: List[str] = []
    for row in rows:
        for key in row:
            if key not in available:
                available.append(key)
    chosen = (
        (_resolve_columns(columns, available) if columns else [])
        or _assertion_columns(assertions, available)
        or available
    )
    return {
        "columns": chosen,
        "rows": [_display_row(r, chosen) for r in rows],
        "ordered": detect_ordered(sql, dialect),
    }


# ── Comparaison au replay ─────────────────────────────────────────────────────


def compare_expect(
    expect: Dict[str, Any], actual_rows: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """Compare la sortie observée au contrat ``expect`` (multiset, ou liste ordonnée
    si ``ordered``). Déterministe, zéro LLM.

    Retourne ``{"passed", "ordered", "expected_count", "actual_count", "missing",
    "unexpected", "order_only_mismatch"}`` — ``missing`` = lignes du contrat absentes
    de la sortie, ``unexpected`` = lignes de la sortie hors contrat, chacune projetée
    sur ``expect.columns`` et cappée à ``DIFF_CAP``. ``order_only_mismatch`` : mêmes
    lignes, ordre différent — signal d'ex-æquo sur la clé de tri (sortie
    non-déterministe ; parade produit : rendre les données discriminantes, axe `tie`).
    """
    columns: List[str] = list(expect.get("columns") or [])
    expected_rows: List[Dict[str, Any]] = list(expect.get("rows") or [])
    ordered = bool(expect.get("ordered"))

    expected_canon = [_canon_row(r, columns) for r in expected_rows]
    actual_canon = [_canon_row(r, columns) for r in actual_rows]

    if ordered:
        passed = expected_canon == actual_canon
    else:
        passed = Counter(expected_canon) == Counter(actual_canon)

    missing: List[Dict[str, Any]] = []
    unexpected: List[Dict[str, Any]] = []
    if not passed:
        # Le diff est toujours calculé en multiset (lisible même en mode ordonné :
        # une simple permutation → missing/unexpected vides + passed=False).
        expected_counter = Counter(expected_canon)
        actual_counter = Counter(actual_canon)
        missing_counter = expected_counter - actual_counter
        unexpected_counter = actual_counter - expected_counter
        for row, canon in zip(expected_rows, expected_canon):
            if missing_counter.get(canon):
                missing_counter[canon] -= 1
                missing.append(_display_row(row, columns))
                if len(missing) >= DIFF_CAP:
                    break
        for row, canon in zip(actual_rows, actual_canon):
            if unexpected_counter.get(canon):
                unexpected_counter[canon] -= 1
                unexpected.append(_display_row(row, columns))
                if len(unexpected) >= DIFF_CAP:
                    break

    return {
        "passed": passed,
        "ordered": ordered,
        "expected_count": len(expected_rows),
        "actual_count": len(actual_rows),
        "missing": missing,
        "unexpected": unexpected,
        "order_only_mismatch": (
            not passed and Counter(expected_canon) == Counter(actual_canon)
        ),
    }


# ── Dual-write à la persistance ───────────────────────────────────────────────


def sync_expect_on_doc(doc: Dict[str, Any], previous_sql: Optional[str] = None) -> None:
    """Dual-write du contrat sur un doc de test au moment de sa persistance.

    Pour chaque cas portant une sortie observée (``results_json``) : rafraîchit
    ``expect`` et pose ``review.status = "draft"`` si absent. Ne touche JAMAIS un
    contrat ``confirmed`` (gelé par l'humain) ni ``stale`` (le vieux contrat est la
    base du diff de re-confirmation), ni un cas mort-né (sa sortie vide est un
    artefact d'échec, pas un contrat).

    ``previous_sql`` (le SQL de la définition déjà sur disque) : s'il diffère du SQL
    entrant, les contrats ``confirmed`` basculent en ``stale`` — le SQL a changé, la
    confirmation ne vaut plus, le prochain replay montrera le diff ancien contrat vs
    nouvelle sortie (écran de détection de régression). Best-effort : ne lève jamais.
    """
    from storage.test_files import is_deadborn_case

    sql = doc.get("sql") or ""
    sql_changed = (
        previous_sql is not None
        and sql.strip() != ""
        and previous_sql.strip() != ""
        and previous_sql.strip() != sql.strip()
    )
    for case in doc.get("test_cases") or []:
        if not isinstance(case, dict):
            continue
        try:
            review = case.get("review") if isinstance(case.get("review"), dict) else {}
            if review.get("status") == "confirmed" and sql_changed:
                case["review"] = {**review, "status": "stale"}
                continue
            if review.get("status") in ("confirmed", "stale"):
                continue
            if is_deadborn_case(case):
                continue
            if "results_json" not in case:
                continue
            # Colonnes déjà choisies (coherence_check, spec §5, ou édition humaine) :
            # collantes au rafraîchissement d'un draft — on refresh les LIGNES sans
            # perdre la sélection de colonnes porteuses.
            prev = case.get("expect") if isinstance(case.get("expect"), dict) else None
            prev_columns = prev.get("columns") if prev else None
            expect = build_expect(
                case.get("results_json"),
                case.get("assertion_results"),
                sql,
                columns=prev_columns or None,
            )
            if expect is None:
                continue
            case["expect"] = expect
            if review.get("status") != "draft":
                case["review"] = {**review, "status": "draft"}
        except Exception:
            continue


def confirm_case(
    case: Dict[str, Any], sql: str, dialect: Optional[str] = None
) -> Dict[str, Any]:
    """Confirmation humaine d'un cas (endpoint ``confirm`` / CLI ``mocksql confirm``).

    Gèle la sortie ACTUELLEMENT observée comme contrat : reconstruit ``expect`` depuis
    ``results_json`` (c'est ce que l'utilisateur a sous les yeux — après un ``stale``,
    c'est la nouvelle sortie qu'il accepte), avec repli sur l'``expect`` déjà stocké
    quand la sortie n'est plus en cache (clone CI). Pose ``review.status="confirmed"``
    + ``confirmed_by="user"``. Pure (copie) ; lève ``ValueError`` si rien à geler.
    """
    from storage.test_files import is_deadborn_case

    expect = None
    if not is_deadborn_case(case) and "results_json" in case:
        expect = build_expect(
            case.get("results_json"), case.get("assertion_results"), sql, dialect
        )
    if expect is None and isinstance(case.get("expect"), dict):
        expect = case["expect"]
    if expect is None:
        raise ValueError(
            "Rien à confirmer : ce cas n'a ni sortie observée exploitable ni contrat "
            "expect existant (relance le test d'abord)."
        )
    review = case.get("review") if isinstance(case.get("review"), dict) else {}
    confirmed = dict(case)
    confirmed["expect"] = expect
    confirmed["review"] = {
        **review,
        "status": "confirmed",
        "confirmed_by": "user",
        "confirmed_at": datetime.now().isoformat(),
    }
    return confirmed
