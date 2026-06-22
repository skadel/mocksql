"""SPEC 4 — contraintes par branche (UNION ALL), pas d'AND aplati cross-branches.

Vérifie deux invariants (déjà tenus par l'existant, verrouillés ici en régression) :

1. Sur le SQL **complet**, ``build_conditions_hint`` présente les branches d'un UNION
   ALL **par branche** (libellés ``[Branch N] … | …``), jamais en un seul ``AND`` qui
   mêlerait des prédicats mutuellement exclusifs (``region='NORTH' AND region='SOUTH'``).
2. Sur le SQL **slicé** à une branche (``slice_to_path``), le ``conditions`` ne contient
   que les prédicats atteignables depuis cette branche — les prédicats des branches
   sœurs sont physiquement absents.

Le générateur calcule ``constraints_hint`` sur le SQL **déjà slicé** (cf.
``examples_generator`` : ``resolve_active_sql`` rebinde ``optimized_sql`` sur la branche
ciblée avant ``_run_simplify_and_hint``) → chaque test focalisé reçoit des contraintes
cohérentes avec sa branche. Ces tests garantissent que ça reste vrai.
"""

import sqlglot

from build_query.constraint_simplifier import build_conditions_hint
from build_query.path_slicer import slice_to_path

DIALECT = "bigquery"

# UNION ALL final ; les deux branches filtrent des valeurs MUTUELLEMENT EXCLUSIVES
# (NORTH/2017 vs SOUTH/2018) : un AND aplati serait insatisfaisable.
TWO_BRANCH_SQL = """
WITH base AS (SELECT id, region, yr FROM proj.ds.sales)
SELECT id, 'north' AS kind FROM base WHERE region = 'NORTH' AND yr = 2017
UNION ALL
SELECT id, 'south' AS kind FROM base WHERE region = 'SOUTH' AND yr = 2018
"""

NO_UNION_SQL = """
SELECT id FROM proj.ds.sales WHERE region = 'NORTH' AND yr = 2017
"""


def _decompose(sql: str, dialect: str = DIALECT) -> list[dict]:
    parsed = sqlglot.parse_one(sql, read=dialect)
    nodes: list[dict] = []
    for cte in parsed.ctes:
        nodes.append({"name": cte.alias_or_name, "code": cte.this.sql(dialect=dialect)})
    body = parsed.copy()
    body.args.pop("with", None)
    body.args.pop("with_", None)
    nodes.append({"name": "final_query", "code": body.sql(dialect=dialect)})
    return nodes


def test_full_sql_conditions_are_labeled_per_branch_not_flat_and():
    conditions = build_conditions_hint(TWO_BRANCH_SQL, DIALECT).get("conditions") or ""
    # Branches présentées séparément (pas un seul AND mêlant NORTH et SOUTH).
    assert "[Branch 1]" in conditions and "[Branch 2]" in conditions
    assert "NORTH" in conditions and "SOUTH" in conditions
    # Le piège à éviter : NORTH et SOUTH AND-és dans le même groupe satisfiable.
    assert "'NORTH' AND yr = 2018" not in conditions
    assert "'SOUTH' AND yr = 2017" not in conditions


def test_sliced_branch_conditions_exclude_sibling_predicates():
    sliced = slice_to_path(TWO_BRANCH_SQL, "north", _decompose(TWO_BRANCH_SQL), DIALECT)
    conditions = build_conditions_hint(sliced, DIALECT).get("conditions") or ""
    assert "NORTH" in conditions and "2017" in conditions
    # Prédicats de la branche sœur absents (slice physique).
    assert "SOUTH" not in conditions
    assert "2018" not in conditions
    # Plus de séparateur de branches : une seule branche subsiste.
    assert "[Branch" not in conditions


def test_non_union_conditions_unchanged():
    conditions = build_conditions_hint(NO_UNION_SQL, DIALECT).get("conditions") or ""
    assert "NORTH" in conditions and "2017" in conditions
    assert (
        "[Branch" not in conditions
    )  # aucun balisage de branche sur une requête plate
