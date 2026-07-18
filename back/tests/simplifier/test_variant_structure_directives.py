"""Directives de format par colonne dérivées de l'AST (`column_directives`).

Diagnostic éval Spider2-snow (sf_bq091/099/216/444) : des données plausibles
passent l'INSERT mais sont filtrées à zéro ligne parce que leur format ne
correspond pas au parsing date/JSON de la requête. Le SQL est la vérité du
format — ces tests vérifient que `build_conditions_hint` :

1. capture `TO_DATE(CAST(col AS VARCHAR), fmt)` malgré le cast intercalé
   (sf_bq216 — le skip anti-double-comptage sautait tout le pattern) ;
2. dérive la structure JSON attendue des colonnes VARIANT depuis les accès
   FLATTEN / GET_PATH / bracket (sf_bq099, sf_bq444) ;
3. expose le tout en `column_directives` (structuré, pour les descriptions
   Pydantic) et en `format_constraints` (strings, pour le hint prompt).
"""

from build_query.constraint_simplifier import build_conditions_hint

# ── sf_bq216 : TO_DATE à travers un cast string no-op ─────────────────────────

_SQL_TODATE_THROUGH_CAST = """
SELECT 1
FROM "P"."P"."PUBLICATIONS" p
WHERE p."filing_date" <> 0
  AND EXTRACT(YEAR FROM TO_DATE(CAST(p."filing_date" AS VARCHAR), 'YYYYMMDD')) = 2016
"""


def test_todate_through_string_cast_emits_format_constraint():
    hint = build_conditions_hint(_SQL_TODATE_THROUGH_CAST, dialect="snowflake")
    fc = hint.get("format_constraints") or []
    assert any("filing_date" in s and "TO_DATE" in s and "%Y%m%d" in s for s in fc), fc


def test_todate_through_string_cast_emits_column_directive():
    hint = build_conditions_hint(_SQL_TODATE_THROUGH_CAST, dialect="snowflake")
    directives = hint.get("column_directives") or {}
    entry = directives.get("publications.filing_date")
    assert entry, directives
    assert any(d["kind"] == "date_format" and d["format"] == "%Y%m%d" for d in entry), (
        entry
    )


def test_bare_tsordstodate_wrapper_not_captured():
    """Un TO_DATE/wrapper sans format explicite ne doit rien émettre (sqlglot
    insère des TsOrDsToDate implicites sans format)."""
    hint = build_conditions_hint(
        'SELECT 1 FROM "P"."P"."T" t WHERE TO_DATE(t."d") = \'2020-01-01\'',
        dialect="snowflake",
    )
    fc = hint.get("format_constraints") or []
    assert not any("TO_DATE" in s for s in fc), fc


def test_nested_format_fn_still_skipped():
    """Non-régression : la contrainte appartient à la fonction INTERNE seulement
    (FORMAT_DATE par-dessus PARSE_DATE ne doit pas être attribué à la colonne)."""
    hint = build_conditions_hint(
        "SELECT FORMAT_DATE('%d/%m', PARSE_DATE('%d%b%Y', t.col)) AS f FROM tbl t",
        dialect="bigquery",
    )
    fc = hint.get("format_constraints") or []
    parse_entries = [s for s in fc if "PARSE_DATE" in s]
    format_entries = [s for s in fc if "FORMAT_DATE" in s]
    assert parse_entries, fc
    assert not format_entries, fc


# ── sf_bq099 : FLATTEN + GET_PATH(value, 'champ') → tableau d'objets ───────────

_SQL_FLATTEN_OBJECTS = """
SELECT UPPER("fa"."VALUE":"name"::string) AS assignee_name
FROM "PATENTS"."PATENTS"."PUBLICATIONS" AS "p",
     LATERAL FLATTEN(input => "p"."cpc") AS "fc",
     LATERAL FLATTEN(input => "p"."assignee_harmonized") AS "fa"
WHERE UPPER("fc"."VALUE":"code"::string) LIKE 'A01B3%'
"""


def test_flatten_getpath_emits_object_array_directive():
    hint = build_conditions_hint(_SQL_FLATTEN_OBJECTS, dialect="snowflake")
    directives = hint.get("column_directives") or {}
    cpc = directives.get("publications.cpc")
    assert cpc and any(
        d["kind"] == "json_object_array" and d["fields"] == ["code"] for d in cpc
    ), directives
    ah = directives.get("publications.assignee_harmonized")
    assert ah and any(
        d["kind"] == "json_object_array" and d["fields"] == ["name"] for d in ah
    ), directives


def test_flatten_object_array_in_format_constraints_strings():
    hint = build_conditions_hint(_SQL_FLATTEN_OBJECTS, dialect="snowflake")
    fc = hint.get("format_constraints") or []
    assert any("cpc" in s and "'code'" in s for s in fc), fc


# ── sf_bq216 : FLATTEN sans accès champ → tableau de scalaires ────────────────

_SQL_FLATTEN_SCALARS = """
SELECT f.index, f.value::FLOAT AS v
FROM "PG"."PG"."ABS_AND_EMB" AS t,
     LATERAL FLATTEN(input => t."embedding_v1") AS f
"""


def test_flatten_scalar_emits_array_directive():
    hint = build_conditions_hint(_SQL_FLATTEN_SCALARS, dialect="snowflake")
    directives = hint.get("column_directives") or {}
    emb = directives.get("abs_and_emb.embedding_v1")
    assert emb and any(d["kind"] == "json_array" for d in emb), directives


# ── sf_bq444 : accès bracket direct → tableau ─────────────────────────────────

_SQL_BRACKET = """
SELECT 1 FROM "CRYPTO"."CRYPTO_ETHEREUM"."LOGS"
WHERE "topics"[0]::STRING = '0xAAA'
"""


def test_bracket_index_emits_array_directive():
    hint = build_conditions_hint(_SQL_BRACKET, dialect="snowflake")
    directives = hint.get("column_directives") or {}
    topics = directives.get("logs.topics")
    assert topics and any(d["kind"] == "json_array" for d in topics), directives


# ── sf_bq182 : champ lu sur la colonne de base (sans FLATTEN) → objet ─────────

_SQL_OBJECT_FIELD = """
SELECT "repo":"name"::string AS repo_name
FROM "GH"."YEAR"."EVENTS"
WHERE "type" = 'PullRequestEvent'
"""


def test_json_field_on_base_column_emits_object_directive():
    hint = build_conditions_hint(_SQL_OBJECT_FIELD, dialect="snowflake")
    directives = hint.get("column_directives") or {}
    repo = directives.get("events.repo")
    assert repo and any(
        d["kind"] == "json_object" and d["fields"] == ["name"] for d in repo
    ), directives


def test_no_directives_key_when_nothing_detected():
    """Pas de bruit : requête sans date/JSON → pas de clé column_directives."""
    hint = build_conditions_hint("SELECT a FROM t WHERE t.a > 3", dialect="snowflake")
    assert "column_directives" not in hint


# ── Cas gold réels : les 3 trous révélés par le probe sur les SQL complets ─────

_SQL_ALIAS_COLLISION = """
WITH tgt AS (
  SELECT f.index, f.value::FLOAT AS v
  FROM "PG"."PG"."ABS_AND_EMB" AS t,
       LATERAL FLATTEN(input => t."embedding_v1") AS f
  WHERE t."publication_number" = 'X'
), cand AS (
  SELECT p."publication_number", f.index, f.value::FLOAT AS v
  FROM "PG"."PG"."PUBLICATIONS" AS p,
       LATERAL FLATTEN(input => p."embedding_v2") AS f
)
SELECT 1 FROM cand c JOIN tgt ON c.index = tgt.index
"""


def test_same_flatten_alias_in_two_ctes_keeps_both_sources():
    """sf_bq216 réel : deux CTEs aliasent leur FLATTEN `f` — un dict keyé par
    alias écrasait la première source et perdait sa directive."""
    hint = build_conditions_hint(_SQL_ALIAS_COLLISION, dialect="snowflake")
    directives = hint.get("column_directives") or {}
    assert "abs_and_emb.embedding_v1" in directives, directives
    assert "publications.embedding_v2" in directives, directives


_SQL_BRACKET_IN_CTE = """
WITH events AS (
  SELECT "topics"[0]::STRING AS t0, "block_number"
  FROM "CRYPTO"."CRYPTO_ETHEREUM"."LOGS"
  WHERE "topics"[0]::STRING = '0xAAA'
)
SELECT t0 FROM events WHERE "block_number" > 0
"""


def test_bracket_unqualified_column_resolved_per_select_scope():
    """sf_bq444 réel : `topics` non qualifié dans un SELECT sur UNE table de base,
    mais la requête compte aussi des CTEs — la table par défaut doit être calculée
    par scope SELECT (les noms de CTE ne sont pas des tables candidates)."""
    hint = build_conditions_hint(_SQL_BRACKET_IN_CTE, dialect="snowflake")
    directives = hint.get("column_directives") or {}
    topics = directives.get("logs.topics")
    assert topics and any(d["kind"] == "json_array" for d in topics), directives


_SQL_TABLE_FLATTEN_MULTITABLE = """
WITH primary_languages AS (
  SELECT "repo_name", lang_data.value:"name"::string AS primary_language
  FROM "GH"."GITHUB_REPOS"."LANGUAGES",
       TABLE(FLATTEN(PARSE_JSON("language"))) AS lang_data
  WHERE "language" <> '[]'
), pr AS (
  SELECT "repo":"name"::string AS repo_name
  FROM "GH"."YEAR"."_2023"
)
SELECT 1
FROM primary_languages plo
JOIN pr ON plo."repo_name" = pr.repo_name
"""


def test_table_flatten_unqualified_source_scoped_to_select():
    """sf_bq182 réel : source FLATTEN non qualifiée (`PARSE_JSON("language")`) dans
    une requête multi-tables — résolue par scope SELECT ; les champs lus sur l'alias
    (`value:"name"`) reviennent à la colonne source, pas à une pseudo-table
    `lang_data`."""
    hint = build_conditions_hint(_SQL_TABLE_FLATTEN_MULTITABLE, dialect="snowflake")
    directives = hint.get("column_directives") or {}
    lang = directives.get("languages.language")
    assert lang and any(
        d["kind"] == "json_object_array" and d["fields"] == ["name"] for d in lang
    ), directives
    repo = directives.get("_2023.repo")
    assert repo and any(
        d["kind"] == "json_object" and d["fields"] == ["name"] for d in repo
    ), directives
    # pas de fuite de l'alias FLATTEN en pseudo-table
    assert not any(k.startswith("lang_data.") for k in directives), directives
