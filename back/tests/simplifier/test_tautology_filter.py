"""
Tests for tautology/contradiction filtering in build_conditions_hint().

A predicate comparing a term with itself carries no usable signal:
  • ``X = X``  → always true, constrains nothing.
  • ``X <> X`` → always false, an impossible contradiction.

Injected into the generator prompt they are pure noise — at worst they push the
model to make a value differ from itself. They must be dropped from the
``conditions`` hint, while genuine self-joins (``a.x = b.x`` on distinct aliases)
and ordinary predicates are preserved.
"""

from build_query.constraint_simplifier import build_conditions_hint


def _conditions(sql: str) -> str:
    return build_conditions_hint(sql, dialect="bigquery").get("conditions", "")


def test_self_equality_is_dropped():
    sql = "SELECT a FROM t WHERE t.a = t.a AND t.b = 'x'"
    conditions = _conditions(sql)
    assert "t.a = t.a" not in conditions
    # the legitimate predicate survives
    assert "'x'" in conditions


def test_self_inequality_is_dropped():
    sql = "SELECT a FROM t WHERE t.a <> t.a AND t.b = 'x'"
    conditions = _conditions(sql)
    assert "t.a <> t.a" not in conditions
    assert "t.a != t.a" not in conditions
    assert "'x'" in conditions


def test_genuine_self_join_is_preserved():
    # Distinct aliases of the same base table → NOT a tautology, must remain.
    sql = "SELECT a.id FROM t AS a JOIN t AS b ON a.id = b.parent_id"
    conditions = _conditions(sql)
    assert "id" in conditions and "parent_id" in conditions


def test_ordinary_equality_is_preserved():
    sql = "SELECT a FROM t WHERE t.col = 'OUVERTURE'"
    assert "'OUVERTURE'" in _conditions(sql)


# ── Collapse post-résolution de lineage ──────────────────────────────────────
# Deux colonnes de CTE DISTINCTES (regpment_new / regpment_old) dont les lineages
# remontent à la même colonne de base (cartes.regroupement). Le prédicat brut est
# une vraie contrainte métier (les deux valeurs doivent différer) ; après
# résolution il s'écraserait en `cartes.regroupement <> cartes.regroupement`
# (contradiction impossible → le générateur ne peut pas la satisfaire). On doit
# rendre la forme brute alias-qualifiée, pas la forme collapsée.

_SHARED_LINEAGE_SQL = """
WITH photo_m AS (
  SELECT t.id AS id, t.regroupement AS regpment_new
  FROM proj.ds.cartes AS t
), photo_m_1 AS (
  SELECT t.id AS id, t.regroupement AS regpment_old
  FROM proj.ds.cartes AS t
), meg AS (
  SELECT a.regpment_new, b.regpment_old
  FROM photo_m AS a
  JOIN photo_m_1 AS b ON a.id = b.id
  WHERE a.regpment_new {op} b.regpment_old
)
SELECT * FROM meg
"""


def test_lineage_collapse_inequality_keeps_raw_form():
    conditions = _conditions(_SHARED_LINEAGE_SQL.format(op="<>"))
    assert "cartes.regroupement <> cartes.regroupement" not in conditions
    assert "cartes.regroupement != cartes.regroupement" not in conditions
    # la contrainte métier (valeurs différentes) survit sous sa forme brute
    assert "regpment_new" in conditions and "regpment_old" in conditions


def test_lineage_collapse_equality_keeps_raw_form():
    conditions = _conditions(_SHARED_LINEAGE_SQL.format(op="="))
    assert "cartes.regroupement = cartes.regroupement" not in conditions
    assert "regpment_new" in conditions and "regpment_old" in conditions
