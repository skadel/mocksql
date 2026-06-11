"""
P1b — Prédicats sur colonnes dérivées : ne pas remapper vers la colonne de base.

``_resolve_via_lineage`` retient la dernière feuille Table de l'arbre de lineage
comme (base_table, base_col). Pour une colonne DÉRIVÉE — ex.
``typ_client = CASE WHEN SUM(nb_contrats_new) > 0 …`` où
``nb_contrats_new = COUNT(no_carte)`` — la marche descend à travers le CASE et
les agrégats jusqu'à ``ds_ref_porteur.no_carte``. Le prédicat
``temp_view_client.typ_client = 'OUVERTURE'`` était alors rendu
``ds_ref_porteur.no_carte = 'OUVERTURE'`` : faux et activement nuisible, dans la
chaîne ``conditions`` ET dans les descriptions de champs Pydantic (via
``source_columns``).

Attendu : quand la résolution traverse une dérivation non-identité (agrégat,
CASE, arithmétique, concat — tout sauf renommage/alias de colonne nue), le
prédicat garde sa forme CTE-qualifiée d'origine, et aucune contrainte n'est
rattachée à la colonne de base.
"""

from build_query.constraint_simplifier import (
    ColumnRef,
    build_conditions_hint,
    simplify,
)

# Mini-pattern de la requête bancaire de l'audit : colonne catégorielle dérivée
# par CASE sur agrégat d'agrégat, filtrée par littéral dans la CTE suivante.
_DERIVED_CASE_SQL = """
WITH stats AS (
  SELECT p.no_personne, COUNT(p.no_carte) AS nb_contrats_new
  FROM proj.ds.ds_ref_porteur AS p
  GROUP BY p.no_personne
), temp_view_client AS (
  SELECT s.no_personne,
         CASE WHEN SUM(s.nb_contrats_new) > 0 THEN 'OUVERTURE' ELSE 'FERMETURE' END AS typ_client
  FROM stats AS s
  GROUP BY s.no_personne
)
SELECT * FROM temp_view_client AS t WHERE t.typ_client = 'OUVERTURE'
"""


def test_conditions_keep_cte_form_for_derived_column():
    hint = build_conditions_hint(_DERIVED_CASE_SQL, dialect="bigquery")
    conditions = hint.get("conditions", "")
    assert "no_carte = 'OUVERTURE'" not in conditions
    assert "no_personne = 'OUVERTURE'" not in conditions
    # la forme CTE-qualifiée d'origine figure à la place
    assert "typ_client = 'OUVERTURE'" in conditions


def test_no_pydantic_constraint_on_base_column_for_derived_predicate():
    result = simplify(_DERIVED_CASE_SQL, dialect="bigquery")
    for ref, constraints in result.source_columns.items():
        for c in constraints:
            if c.op == "eq" and c.value == "OUVERTURE":
                assert ref.column != "no_carte", (
                    f"contrainte 'OUVERTURE' remappée sur {ref}"
                )
                assert ref.column != "no_personne", (
                    f"contrainte 'OUVERTURE' remappée sur {ref}"
                )


def test_pure_rename_still_resolves_to_base_table():
    # Garde-fou anti « trop strict » : une chaîne de purs renommages reste
    # résolue vers la table de base (le hint garde ses ancres base-table).
    sql = """
    WITH c1 AS (
      SELECT t.a AS x FROM proj.ds.t AS t
    ), c2 AS (
      SELECT c1.x AS y FROM c1
    )
    SELECT * FROM c2 WHERE c2.y = 'v'
    """
    conditions = build_conditions_hint(sql, dialect="bigquery").get("conditions", "")
    assert "t.a = 'v'" in conditions


def test_arithmetic_derivation_keeps_cte_form():
    sql = """
    WITH c1 AS (
      SELECT t.amount + t.fee AS total FROM proj.ds.t AS t
    )
    SELECT * FROM c1 WHERE c1.total > 100
    """
    conditions = build_conditions_hint(sql, dialect="bigquery").get("conditions", "")
    assert "amount > 100" not in conditions
    assert "fee > 100" not in conditions
    assert "total > 100" in conditions


def test_is_identity_flag_exposed_on_columnref():
    # L'indicateur est porté par ColumnRef (défaut True, hors égalité/hash).
    ref = ColumnRef("t", "a")
    assert ref.is_identity is True
    assert ColumnRef("t", "a", is_identity=False) == ref
