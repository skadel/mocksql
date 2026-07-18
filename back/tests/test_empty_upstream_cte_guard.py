"""Fix 3 — garde-fou « CTE amont vide » (sf_bq093).

Un résultat final NON vide peut masquer une CTE amont REQUISE vide : l'agrégat d'un
ensemble vide rend 1 ligne NULL, puis un LEFT JOIN + COALESCE fabrique des lignes
d'échafaudage (net_change=0, address NULL). Le circuit `empty_results` de l'executor ne
se déclenche jamais (le résultat n'est pas vide) et les assertions blanchies sur ces
lignes dégénérées laissent le test en « Insuffisant » mort (reason_type None → aucune
boucle de correction).

`probe_empty_upstream_cte` / `_scan_blocking_empty_cte` rejouent le CTE-trace à la demande
(uniquement sur verdict Insuffisant) et reclassent en `bad_data` quand une CTE requise est
vide sur un test happy-path — sans jamais flaguer une CTE vide optionnelle (LEFT/anti-join).
"""

import duckdb
import pytest

from build_query.examples_executor import (
    _build_empty_cte_diagnostic,
    _is_happy_path_test,
    _scan_blocking_empty_cte,
)
from utils.test_utils import EMPTY_RESULT_SENTINEL_SQL

_SUFFIX = "t1"

# Squelette minimal de sf_bq093 : FILTERED_TX filtre sur un jour, ADDRESS_CHANGES agrège,
# final_query prend MAX sur un ensemble éventuellement vide (→ 1 ligne NULL, non vide).
_CTES = [
    {
        "name": "FILTERED_TX",
        "code": (
            "SELECT t.id AS id, t.value AS value "
            "FROM proj.eth.tx AS t "
            "WHERE t.status = 1 AND t.day = '2016-10-14'"
        ),
    },
    {
        "name": "ADDRESS_CHANGES",
        "code": (
            "SELECT f.id AS id, SUM(f.value) AS net FROM FILTERED_TX AS f GROUP BY f.id"
        ),
    },
    {
        "name": "final_query",
        "code": "SELECT s.net AS net FROM (SELECT MAX(net) AS net FROM ADDRESS_CHANGES) AS s",
    },
]


def _tx_con(day_value: str):
    c = duckdb.connect()
    c.execute(
        f"CREATE TABLE eth_tx_{_SUFFIX} (id TEXT, value INTEGER, status INTEGER, day TEXT)"
    )
    c.execute(f"INSERT INTO eth_tx_{_SUFFIX} VALUES ('a', 10, 1, ?)", [day_value])
    return c


# ── _is_happy_path_test (pur) ────────────────────────────────────────────────


def test_happy_path_requires_must_hold():
    assert _is_happy_path_test({"branch_plan": {"must_hold": ["x produit des lignes"]}})
    assert not _is_happy_path_test({"branch_plan": {"must_hold": []}})
    assert not _is_happy_path_test({})


def test_happy_path_excludes_intentional_empty_sentinel():
    test = {
        "branch_plan": {"must_hold": ["x"]},
        "assertion_results": [{"sql": EMPTY_RESULT_SENTINEL_SQL, "passed": True}],
    }
    assert not _is_happy_path_test(test)


# ── _scan_blocking_empty_cte (intégration DuckDB) ────────────────────────────


@pytest.mark.asyncio
async def test_scan_detects_required_empty_cte():
    # jour injecté != jour filtré → FILTERED_TX vide → ADDRESS_CHANGES vide (toutes deux
    # requises), mais final_query (MAX sur vide) rend 1 ligne NULL → non vide.
    con = _tx_con("2016-10-13")
    scan = await _scan_blocking_empty_cte(_CTES, _SUFFIX, "proj", "bigquery", con)
    assert scan is not None
    failing_cte, cte_trace = scan
    # la racine du vide (topo) est FILTERED_TX
    assert failing_cte == "FILTERED_TX"
    assert cte_trace["FILTERED_TX"]["row_count"] == 0
    assert cte_trace["FILTERED_TX"].get("blocking") is True


@pytest.mark.asyncio
async def test_scan_returns_none_when_ctes_non_empty():
    # jour injecté == jour filtré → FILTERED_TX non vide → aucune CTE requise bloquante.
    con = _tx_con("2016-10-14")
    scan = await _scan_blocking_empty_cte(_CTES, _SUFFIX, "proj", "bigquery", con)
    assert scan is None


@pytest.mark.asyncio
async def test_scan_ignores_optional_left_joined_empty_cte():
    # OPT est vide mais seulement LEFT-jointe → résultat métier valide, pas un blocage.
    ctes = [
        {
            "name": "OPT",
            "code": "SELECT t.id AS id FROM proj.eth.tx AS t WHERE t.day = '2999-01-01'",
        },
        {
            "name": "final_query",
            "code": (
                "SELECT m.id AS id, o.id AS oid "
                "FROM proj.eth.main AS m LEFT JOIN OPT AS o ON m.id = o.id"
            ),
        },
    ]
    con = _tx_con("2016-10-14")
    con.execute(f"CREATE TABLE eth_main_{_SUFFIX} (id TEXT)")
    con.execute(f"INSERT INTO eth_main_{_SUFFIX} VALUES ('m1')")
    scan = await _scan_blocking_empty_cte(ctes, _SUFFIX, "proj", "bigquery", con)
    assert scan is None


# ── _build_empty_cte_diagnostic (pur) ────────────────────────────────────────


def test_diagnostic_has_agent_keys_and_names_cte():
    cte_trace = {
        "FILTERED_TX": {"row_count": 0, "blocking": True},
        "ADDRESS_CHANGES": {"row_count": 0, "blocking": True},
    }
    diag = _build_empty_cte_diagnostic("FILTERED_TX", cte_trace, _CTES, "bigquery")
    # toutes les clés lues par _build_agent_eval_context
    for key in (
        "root_cause",
        "sql_pattern",
        "data_issue",
        "fix_summary",
        "fix_recipe",
        "affected_tables",
        "affected_ctes",
    ):
        assert key in diag
    assert diag["sql_pattern"] == "empty_upstream_cte"
    assert diag["affected_ctes"] == ["FILTERED_TX"]
    assert "FILTERED_TX" in diag["data_issue"]
