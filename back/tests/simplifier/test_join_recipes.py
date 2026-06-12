"""
P1a — Recettes de jointure pré-calculées (clés dérivées).

Quand une clé de JOIN est le produit d'une transformation (CASE, SUBSTR+TRIM,
CAST…), demander au LLM d'inverser mentalement la transformation échoue
(audit requête bancaire : `cd_chef_file` attendu '1' mais généré 'BP',
`cd_type_carte_smp` attendu 'ROD' après SUBSTR…). Ces transformations sont
déterministes : l'inversion est pré-calculée côté Python (énumération CASE,
vérification forward DuckDB, contrainte de format CAST) et injectée comme
consigne concrète dans `<constraints>`.
"""

from build_query.join_recipes import build_join_recipes, build_join_recipes_block

# ── CASE 100 % littéral → énumération des couples (source → clé) ─────────────

_CASE_SQL = """
WITH corr_cartes AS (
  SELECT c.no_carte,
         CASE WHEN c.reseau = 'BP' THEN '1'
              WHEN c.reseau = 'CE' THEN '2'
              ELSE NULL END AS cd_chef_file
  FROM proj.ds.cartes AS c
)
SELECT *
FROM corr_cartes AS cc
JOIN proj.ds.ref_port AS rp ON cc.cd_chef_file = rp.cd_chef_file
"""


def test_case_literal_enumerates_source_to_key_pairs():
    recipes = build_join_recipes(_CASE_SQL, dialect="bigquery")
    assert len(recipes) == 1
    r = recipes[0]
    assert "CASE" in r
    assert "reseau" in r
    assert "'BP'" in r and "'1'" in r
    assert "'CE'" in r and "'2'" in r


# ── Chaîne SUBSTR+TRIM → vérification forward DuckDB ─────────────────────────

_SUBSTR_SQL = """
WITH corr AS (
  SELECT TRIM(SUBSTR(c.code_produit_bpce_ps, 2, LENGTH(c.code_produit_bpce_ps) - 2), '"') AS code_produit
  FROM proj.ds.correspondance_cartes AS c
)
SELECT *
FROM corr AS co
JOIN proj.ds.cartes AS k ON co.code_produit = k.cd_type_carte_smp
"""


def test_string_chain_is_forward_verified_on_duckdb():
    recipes = build_join_recipes(_SUBSTR_SQL, dialect="bigquery")
    assert len(recipes) == 1
    r = recipes[0]
    # la recette porte le couple vérifié (gabarit avec quotes de bord → valeur dépliée)
    assert "vérifié" in r
    assert "'PROD1'" in r and "PROD1" in r
    assert "code_produit_bpce_ps" in r


# ── CAST / SAFE_CAST → contrainte de format des deux côtés ───────────────────

_SAFE_CAST_SQL = """
WITH t1 AS (
  SELECT SAFE_CAST(a.id_str AS INT64) AS id_num FROM proj.ds.a AS a
)
SELECT * FROM t1 JOIN proj.ds.b AS b ON t1.id_num = b.id
"""


def test_safe_cast_emits_format_recipe():
    recipes = build_join_recipes(_SAFE_CAST_SQL, dialect="bigquery")
    assert len(recipes) == 1
    r = recipes[0]
    assert "id_str" in r
    assert "INT64" in r or "numérique" in r


# ── Non-inversible (plusieurs colonnes sources) → recette générique ──────────

_MULTI_COL_SQL = """
WITH t1 AS (
  SELECT CONCAT(a.x, '-', a.y) AS k FROM proj.ds.a AS a
)
SELECT * FROM t1 JOIN proj.ds.b AS b ON t1.k = b.k
"""


def test_non_invertible_falls_back_to_generic_recipe():
    recipes = build_join_recipes(_MULTI_COL_SQL, dialect="bigquery")
    assert len(recipes) == 1
    r = recipes[0]
    # fallback : décrit la dérivation et demande de choisir la valeur source
    assert "dérivée" in r
    assert "l'autre côté" in r


# ── JOIN sur colonnes nues → aucune recette (pas de bruit) ───────────────────

_PLAIN_SQL = """
WITH c1 AS (SELECT t.id AS id, t.lib AS lib FROM proj.ds.t AS t)
SELECT * FROM c1 JOIN proj.ds.u AS u ON c1.id = u.id AND c1.lib = u.lib
"""


def test_plain_column_joins_produce_no_recipe():
    assert build_join_recipes(_PLAIN_SQL, dialect="bigquery") == []


# ── CASE inline dans le ON (sans passer par une CTE) ─────────────────────────

_INLINE_CASE_SQL = """
SELECT *
FROM proj.ds.a AS a
JOIN proj.ds.b AS b
  ON b.k = CASE WHEN a.reseau = 'BP' THEN '1' ELSE '2' END
"""


def test_inline_case_in_on_clause_is_detected():
    recipes = build_join_recipes(_INLINE_CASE_SQL, dialect="bigquery")
    assert len(recipes) == 1
    assert "'BP'" in recipes[0] and "'1'" in recipes[0]


# ── OR … IS NULL sur la clé dérivée : la branche NULL est mentionnée ─────────

_OR_IS_NULL_SQL = """
WITH corr AS (
  SELECT c.no_carte,
         CASE WHEN c.filtre_didd = 'DD' THEN 'D'
              WHEN c.filtre_didd = 'DI' THEN 'I'
              ELSE NULL END AS filtre_didd_key
  FROM proj.ds.cartes AS c
)
SELECT *
FROM corr AS co
JOIN proj.ds.ref AS r
  ON (r.filtre = co.filtre_didd_key OR co.filtre_didd_key IS NULL)
"""


def test_case_with_null_else_mentions_null_branch():
    recipes = build_join_recipes(_OR_IS_NULL_SQL, dialect="bigquery")
    assert len(recipes) == 1
    r = recipes[0]
    assert "'DD'" in r and "'D'" in r
    assert "NULL" in r


# ── Dérivations dégénérées → aucune recette (bruit actif) ────────────────────
# Incident 2026-06-11 (requête bancaire) : le prompt de l'agent contenait
# « cette clé est dérivée par `''` » (branche littérale d'un UNION ALL) et
# « dérivée par `?` » (lineage non résolu sur colonne nue multi-tables) —
# consignes inactionnables qui parasitent la correction.

_UNION_LITERAL_SQL = """
WITH mvt AS (
  SELECT a.id AS id, '' AS regpment FROM proj.ds.a AS a
  UNION ALL
  SELECT b.id AS id, b.regpment AS regpment FROM proj.ds.b AS b
)
SELECT * FROM mvt JOIN proj.ds.ref AS r ON mvt.regpment = r.regroupement
"""


def test_literal_union_branch_produces_no_recipe():
    assert build_join_recipes(_UNION_LITERAL_SQL, dialect="bigquery") == []


_UNRESOLVED_BARE_COL_SQL = """
WITH temp_carte AS (
  SELECT rp.cd_banque AS cd_banque, cd_iban AS cd_iban
  FROM proj.ds.ref_port AS rp
  JOIN proj.ds.evt AS e ON e.no_carte = rp.no_carte
)
SELECT * FROM temp_carte AS t
JOIN proj.ds.cli AS c ON t.cd_iban = c.cd_iban
"""


def test_unresolved_lineage_placeholder_produces_no_recipe():
    recipes = build_join_recipes(_UNRESOLVED_BARE_COL_SQL, dialect="bigquery")
    assert recipes == []


# ── Bloc prompt ──────────────────────────────────────────────────────────────


def test_block_is_empty_without_recipes_and_labeled_with():
    assert build_join_recipes_block(_PLAIN_SQL, dialect="bigquery") == ""
    block = build_join_recipes_block(_CASE_SQL, dialect="bigquery")
    assert "Recettes de jointure" in block
    assert block.count("- ") >= 1


def test_unparseable_sql_returns_empty():
    assert build_join_recipes("not sql at all (", dialect="bigquery") == []
    assert build_join_recipes("", dialect="bigquery") == []
