"""Régression (incident c3) : une assertion générée qui string-slice une colonne
TEMPORELLE sans cast est invalide en DuckDB.

Le LLM a produit `LEFT(partition_date, 10) = '2026-01-01'` pour comparer le préfixe date
d'une colonne TIMESTAMP. DuckDB n'a pas `left(TIMESTAMP, INTEGER)` (seulement
`left(VARCHAR, BIGINT)`) → `Binder Error` au replay. La garde déterministe
`_cast_nontext_string_slicing` enveloppe l'argument non-texte d'un LEFT/RIGHT/SUBSTR dans
`CAST(... AS TEXT)` avant exécution, ce qui rend l'assertion valide tout en restant fidèle
à l'intention (slicer la représentation chaîne). Cf. plan 1-functional-hartmanis.
"""

import duckdb
import pandas as pd

from build_query.assertion_eval import (
    _cast_nontext_string_slicing,
    _evaluate_assertions,
)

_SQL = "SELECT * FROM __result__ WHERE (LEFT(partition_date, 10) = '2026-01-01') IS NOT TRUE"


def test_cast_wraps_nontext_sliced_arg():
    out = _cast_nontext_string_slicing(_SQL, {"partition_date"})
    assert "cast(partition_date as text)" in out.lower()
    assert "left(" in out.lower()  # le LEFT lui-même est conservé


def test_cast_is_idempotent():
    once = _cast_nontext_string_slicing(_SQL, {"partition_date"})
    twice = _cast_nontext_string_slicing(once, {"partition_date"})
    assert twice == once
    # une seule enveloppe CAST, pas de double-wrap.
    assert once.lower().count("cast(partition_date as text)") == 1


def test_cast_leaves_text_column_untouched():
    # `name` n'est PAS dans non_text_cols (colonne string) → LEFT(name, 3) légitime, inchangé.
    sql = "SELECT * FROM __result__ WHERE (LEFT(name, 3) = 'abc') IS NOT TRUE"
    out = _cast_nontext_string_slicing(sql, {"partition_date"})
    assert "cast(" not in out.lower()


def test_cast_unparseable_sql_returned_as_is():
    junk = "this is not sql ;;; (("
    assert _cast_nontext_string_slicing(junk, {"x"}) == junk


def _eval_one(rows, assertion):
    con = duckdb.connect()
    con.register("v", pd.DataFrame(rows))
    return _evaluate_assertions([assertion], "v", con)[0]


def test_evaluate_assertions_left_on_timestamp_passes_after_guard():
    """e2e : vue avec colonne TIMESTAMP + assertion LEFT(partition_date,10).

    AVANT garde → Binder Error (`left(TIMESTAMP, ...)`). APRÈS → passe sans erreur.
    """
    rows = [{"partition_date": pd.Timestamp("2026-01-01"), "id": 1}]
    assertion = {
        "description": "la date de partition correspond",
        "expected_condition": "LEFT(partition_date, 10) = '2026-01-01'",
        "sql": _SQL,
    }
    res = _eval_one(rows, assertion)
    assert not res.get("error"), f"assertion en erreur: {res.get('error')}"
    assert res["passed"] is True
    # le sql persisté est la forme gardée (réplay/stockage propres).
    assert "cast(partition_date as text)" in res["sql"].lower()
