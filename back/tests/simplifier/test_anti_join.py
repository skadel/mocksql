"""
Tests for anti-join constraint handling in constraint_simplifier.

Anti-join patterns:
  • LEFT JOIN cte ON t.col = cte.col WHERE cte.col IS NULL
  • WHERE col NOT IN (SELECT col FROM ...)

Expected behavior:
  1. extract_constraints(): the IS NULL predicate for the anti-join column must NOT
     appear as a positive 'is_null' FilterConstraint — it is a structural exclusion
     marker, not a data constraint. It must appear in col_inequalities.
  2. extract_constraints(): constraints from the anti-join CTE body must NOT be merged
     into the outer group (they define what to exclude, not what to include).
  3. build_conditions_hint(): the conditions string must NOT contain
     "anti_alias.col IS NULL" resolved to a base-table column — this would contradict
     any "IS NOT NULL" constraint on the same column and make the LLM generate
     unsatisfiable data.
  4. build_conditions_hint(): conditions from the anti-join CTE body must NOT appear
     in the conditions hint (they describe the excluded set, not the included set).
  5. build_conditions_hint(): NOT IN (SELECT ...) subquery body conditions must NOT
     appear in the conditions hint.
"""

from build_query.constraint_simplifier import (
    build_conditions_hint,
    extract_constraints,
)


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _tbl_match(ref, name: str) -> bool:
    """Match a ColumnRef against a table name checking both alias and real_table."""
    return ref.table == name or ref.real_table == name


def _ops(groups, table: str, col: str) -> list[str]:
    """Collect all filter ops on table.col across all groups (alias or real_table)."""
    ops = []
    for g in groups:
        for f in g.filters:
            if _tbl_match(f.column, table) and f.column.column == col:
                ops.append(f.op)
    return ops


def _has_col_inequality(groups, t1: str, c1: str, t2: str, c2: str) -> bool:
    for g in groups:
        for a, b in g.col_inequalities:
            if (
                _tbl_match(a, t1)
                and a.column == c1
                and _tbl_match(b, t2)
                and b.column == c2
            ) or (
                _tbl_match(a, t2)
                and a.column == c2
                and _tbl_match(b, t1)
                and b.column == c1
            ):
                return True
    return False


# ─── Bug 1 ── IS NULL must not land in positive filters ───────────────────────


class TestExtractConstraintsAntiJoin:
    """extract_constraints() — LEFT JOIN ... WHERE alias.col IS NULL."""

    SQL = """
    WITH excluded AS (
        SELECT id FROM source_tbl WHERE status = 'active'
    )
    SELECT t.id, t.value
    FROM main_tbl t
    LEFT JOIN excluded e ON t.id = e.id
    WHERE t.id IS NOT NULL
      AND e.id IS NULL
    """

    def test_no_is_null_filter_on_anti_join_column(self):
        """e.id IS NULL must not produce an is_null FilterConstraint."""
        groups = extract_constraints(self.SQL, dialect="bigquery")
        # e.id IS NULL resolves to source_tbl.id (via lineage through the 'excluded' CTE)
        # It must NOT appear as a positive is_null constraint.
        all_is_null_ops = [f for g in groups for f in g.filters if f.op == "is_null"]
        assert all_is_null_ops == [], (
            f"Found unexpected is_null filters: {all_is_null_ops}. "
            "Anti-join IS NULL should not appear as a positive constraint."
        )

    def test_is_not_null_filter_present(self):
        """t.id IS NOT NULL (the real data constraint) must still be captured."""
        groups = extract_constraints(self.SQL, dialect="bigquery")
        ops = _ops(groups, "main_tbl", "id")
        assert "is_not_null" in ops, (
            "t.id IS NOT NULL should produce an is_not_null FilterConstraint."
        )

    def test_anti_join_captured_in_col_inequalities(self):
        """The anti-join pair (t.id, e.id) must be in col_inequalities."""
        groups = extract_constraints(self.SQL, dialect="bigquery")
        has_ineq = _has_col_inequality(groups, "main_tbl", "id", "source_tbl", "id")
        assert has_ineq, (
            "Anti-join ON t.id = e.id WHERE e.id IS NULL must be captured in col_inequalities."
        )

    def test_anti_join_cte_constraints_not_merged(self):
        """Constraints from the anti-join CTE body (status='active') must not appear."""
        groups = extract_constraints(self.SQL, dialect="bigquery")
        all_filters = [f for g in groups for f in g.filters]
        status_filters = [f for f in all_filters if f.column.column == "status"]
        assert status_filters == [], (
            f"Anti-join CTE body constraint 'status = active' must not be merged: {status_filters}"
        )


# ─── Bug 2/3 ── build_conditions_hint ─────────────────────────────────────────


class TestBuildConditionsHintAntiJoin:
    """build_conditions_hint() — anti-join IS NULL must not appear in the hint."""

    SQL = """
    WITH excluded AS (
        SELECT id FROM source_tbl WHERE status = 'active'
    )
    SELECT t.id, t.value
    FROM main_tbl t
    LEFT JOIN excluded e ON t.id = e.id
    WHERE t.id IS NOT NULL
      AND e.id IS NULL
    """

    def test_no_is_null_in_conditions_string(self):
        """The resolved anti-join 'source_tbl.id IS NULL' must not appear in the hint.

        Note: 'NOT main_tbl.id IS NULL' (= IS NOT NULL) is acceptable and should remain.
        The anti-join marker is the bare 'source_tbl.id IS NULL', not the IS NOT NULL.
        """
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        conditions = hint.get("conditions", "")
        # 'source_tbl.id IS NULL' is the anti-join IS NULL resolved to base table — must be absent
        assert "source_tbl.id IS NULL" not in conditions, (
            f"Anti-join IS NULL must not appear in conditions hint: {conditions!r}"
        )

    def test_is_not_null_present_in_conditions(self):
        """The IS NOT NULL constraint on the main table column must still be present.

        sqlglot may render IS NOT NULL as 'NOT x IS NULL' — accept both forms.
        """
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        conditions = hint.get("conditions", "")
        # Accept both 'IS NOT NULL' and 'NOT x IS NULL' forms
        upper = conditions.upper()
        has_not_null = "IS NOT NULL" in upper or (
            "NOT" in upper and "IS NULL" in upper and "main_tbl" in conditions.lower()
        )
        assert has_not_null, (
            f"IS NOT NULL constraint on main_tbl must appear in conditions hint: {conditions!r}"
        )

    def test_anti_join_cte_conditions_not_in_hint(self):
        """Conditions from the anti-join CTE body (status='active') must not appear."""
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        conditions = hint.get("conditions", "")
        assert "active" not in conditions.lower(), (
            f"Anti-join CTE condition 'status = active' must not appear in hint: {conditions!r}"
        )


# ─── anti_joins surfacing ─────────────────────────────────────────────────────


class TestBuildConditionsHintAntiJoinSurfacing:
    """build_conditions_hint() must surface anti-joins in a dedicated `anti_joins`
    key, described in NEGATIVE terms (what to make FALSE), so the generator knows
    the excluded set exists and stops falling into it.

    The excluded criteria must NOT leak into `conditions` (still tested elsewhere),
    but MUST appear in `anti_joins`.
    """

    SQL = """
    WITH excluded AS (
        SELECT id FROM source_tbl WHERE status = 'active'
    )
    SELECT t.id, t.value
    FROM main_tbl t
    LEFT JOIN excluded e ON t.id = e.id
    WHERE t.id IS NOT NULL
      AND e.id IS NULL
    """

    def test_anti_joins_key_present(self):
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        assert hint.get("anti_joins"), (
            f"anti_joins key must be present and non-empty: {hint!r}"
        )

    def test_anti_joins_names_excluded_set(self):
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        blob = " ".join(hint.get("anti_joins", [])).lower()
        assert "excluded" in blob, (
            f"anti_joins must name the excluded set `excluded`: {hint.get('anti_joins')!r}"
        )

    def test_anti_joins_carries_excluded_criteria(self):
        """The excluded set's own criteria (status = 'active') must appear in
        anti_joins so the model knows what to make FALSE."""
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        blob = " ".join(hint.get("anti_joins", [])).lower()
        assert "active" in blob, (
            f"anti_joins must carry the excluded criteria 'status = active': {hint.get('anti_joins')!r}"
        )

    def test_excluded_criteria_still_absent_from_conditions(self):
        """Regression guard: criteria must stay OUT of `conditions`."""
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        assert "active" not in hint.get("conditions", "").lower()


# ─── Bug 5 ── NOT IN (SELECT ...) ─────────────────────────────────────────────


class TestBuildConditionsHintNotIn:
    """build_conditions_hint() — NOT IN (SELECT ...) subquery conditions must not appear."""

    SQL = """
    SELECT t.id, t.value
    FROM main_tbl t
    WHERE t.category = 'B'
      AND t.id NOT IN (SELECT id FROM excluded_tbl WHERE flag = 1)
    """

    def test_not_in_subquery_conditions_excluded_from_hint(self):
        """Conditions inside NOT IN (SELECT ...) must not appear as standalone top-level conditions.

        The NOT IN predicate itself may be rendered inline (including its subquery text),
        but 'flag = 1' must not appear as a separate AND clause outside the subquery.
        """
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        conditions = hint.get("conditions", "")
        # After the fix, the inner SELECT is no longer scanned as a standalone SELECT.
        # 'flag = 1' may still appear inside the inline NOT IN rendering (as part of the
        # subquery SQL text) — that's fine. What must not happen is a separate top-level
        # 'AND flag = 1' clause outside any subquery context.
        # The simplest check: the conditions should not end with or separately have 'flag'
        # as a top-level AND term (i.e., not 'AND flag ...' or 'flag ... AND').
        import re

        # Check there's no top-level 'AND flag' (standalone flag condition)
        has_standalone_flag = bool(
            re.search(r"\bAND\s+flag\b", conditions, re.IGNORECASE)
            or conditions.lower().strip().startswith("flag")
        )
        assert not has_standalone_flag, (
            f"NOT IN subquery condition 'flag = 1' must not appear as standalone top-level condition: {conditions!r}"
        )

    def test_outer_where_condition_present(self):
        """The outer WHERE condition (category = 'B') must still appear."""
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        conditions = hint.get("conditions", "")
        assert (
            "category" in conditions.lower()
            or "'B'" in conditions
            or '"B"' in conditions
        ), f"Outer WHERE condition 'category = B' must appear in hint: {conditions!r}"


# ─── c1.sql pattern reproducer ────────────────────────────────────────────────


class TestC1SqlAntiJoinPattern:
    """Reproduces the exact anti-join pattern from c1.sql that caused empty_results.

    The critical pattern: LEFT JOIN SIRET_ONUS onus ON rcomp.NO_SIRET = onus.NO_SIRET
    WHERE ... AND rcomp.NO_SIRET IS NOT NULL AND onus.NO_SIRET IS NULL
    """

    SQL = """
    WITH FILTERED_TBL AS (
        SELECT id, category, amount
        FROM transactions
        WHERE amount > 0
          AND category IN ('A', 'B', 'C')
    ),
    EXCLUDED_IDS AS (
        SELECT DISTINCT id
        FROM FILTERED_TBL
        WHERE category = 'A'
          AND amount >= 100
    ),
    RESULT AS (
        SELECT f.id, f.category, f.amount
        FROM FILTERED_TBL f
        LEFT JOIN EXCLUDED_IDS excl ON f.id = excl.id
        WHERE f.id IS NOT NULL
          AND excl.id IS NULL
    )
    SELECT * FROM RESULT
    """

    def test_no_is_null_in_filters(self):
        """excl.id IS NULL must not land in positive filters."""
        groups = extract_constraints(self.SQL, dialect="bigquery")
        is_null_filters = [f for g in groups for f in g.filters if f.op == "is_null"]
        assert is_null_filters == [], (
            f"Anti-join IS NULL must not be a positive constraint: {is_null_filters}"
        )

    def test_is_not_null_preserved(self):
        """f.id IS NOT NULL must still be captured."""
        groups = extract_constraints(self.SQL, dialect="bigquery")
        not_null = [f for g in groups for f in g.filters if f.op == "is_not_null"]
        assert len(not_null) >= 1, "IS NOT NULL constraint must be preserved."

    def test_excluded_cte_category_eq_not_merged(self):
        """category = 'A' from EXCLUDED_IDS body must not appear in the outer group."""
        groups = extract_constraints(self.SQL, dialect="bigquery")
        # The outer group should not have the exclusive 'category = A' constraint
        # (which would contradict the outer 'category IN (A, B, C)' being used to
        # generate test data that covers all three categories)
        all_eq_filters = [
            f
            for g in groups
            for f in g.filters
            if f.op == "eq" and f.column.column == "category" and f.value == "A"
        ]
        # If this constraint is present, it means the excluded CTE's category='A'
        # is being merged into the outer query constraints, which is wrong.
        assert all_eq_filters == [], (
            f"Anti-join CTE's category='A' must not be merged into outer constraints: {all_eq_filters}"
        )

    def test_conditions_hint_no_contradiction(self):
        """build_conditions_hint must not produce a positive IS NULL on the anti-join column.

        The anti-join 'excl.id IS NULL' resolves to 'transactions.id IS NULL' (same base
        table as f.id). Having both 'transactions.id IS NULL' and IS NOT NULL is a
        contradiction. After the fix, the bare IS NULL must be absent.
        """
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        conditions = hint.get("conditions", "")
        # The anti-join IS NULL resolved to base table must be absent
        # (sqlglot renders IS NOT NULL as 'NOT x IS NULL' which is fine — just no bare IS NULL)
        import re

        # A bare 'table.col IS NULL' not preceded by NOT — the anti-join marker
        has_bare_is_null = bool(
            re.search(r"(?<![Nn][Oo][Tt]\s)\b\w+\.\w+\s+IS\s+NULL\b", conditions)
        )
        assert not has_bare_is_null, (
            f"Anti-join IS NULL (bare, not 'NOT x IS NULL') must not appear in hint: {conditions!r}"
        )

    def test_conditions_hint_excluded_cte_conditions_absent(self):
        """EXCLUDED_IDS body conditions (amount >= 100) must not appear in hint."""
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        conditions = hint.get("conditions", "")
        # amount >= 100 is from EXCLUDED_IDS — it should not constrain the generated data
        # (the outer query only knows amount > 0)
        assert ">= 100" not in conditions and "100" not in conditions, (
            f"Anti-join CTE condition 'amount >= 100' must not appear in hint: {conditions!r}"
        )


# ── P3.1 — contrat `anti_joins` : clé toujours émise quand un hint existe ─────


class TestAntiJoinsKeyContract:
    """Le SYSTEM du générateur documente la clé `anti_joins` de <constraints> :
    elle doit être émise systématiquement (liste vide incluse) dès qu'un hint
    existe — une clé absente serait ambiguë (« pas d'anti-join » vs
    « extraction incomplète »)."""

    def test_key_present_and_empty_without_anti_join(self):
        hint = build_conditions_hint(
            "SELECT a FROM proj.ds.t AS t WHERE t.b = 'x'", dialect="bigquery"
        )
        assert hint  # un hint existe (conditions)
        assert hint.get("anti_joins") == []

    def test_unparseable_sql_still_returns_empty_dict(self):
        assert build_conditions_hint("", dialect="bigquery") == {}


# ── Régression — anti-join auto-dérivé (pattern SIRET_ONUS de c1.sql) ─────────


class TestSelfDerivedAntiJoin:
    """L'anti-join dont le CTE dérive sa clé de la table sondée elle-même ne
    doit pas contaminer les autres LEFT JOIN du même SELECT.

    Avant le fix, la classification reposait sur les ColumnRefs résolus à la
    table de base : ``onus.no_siret IS NULL`` se résout en ``trans.no_siret``
    (lineage à travers le CTE), la même colonne qui sert de clé à TOUS les
    LEFT JOIN — chaque jointure d'enrichissement était alors marquée
    ``col_inequality`` (« Anti-join (NOT IN) »), en contradiction frontale
    avec le hint ``conditions`` qui exige l'égalité.
    """

    SQL = """
    WITH rcomp AS (
        SELECT no_siret, amount FROM `ds.trans` WHERE amount > 0
    ),
    onus AS (
        SELECT no_siret FROM rcomp WHERE amount > 100
    )
    SELECT r.no_siret
    FROM rcomp r
    LEFT JOIN `ds.clients` c ON r.no_siret = c.id_immatriculation
    LEFT JOIN onus o ON r.no_siret = o.no_siret
    WHERE o.no_siret IS NULL
      AND r.no_siret IS NOT NULL
    """

    def test_enrichment_join_is_not_flagged_as_anti_join(self):
        groups = extract_constraints(self.SQL, dialect="bigquery")
        assert not _has_col_inequality(
            groups, "trans", "no_siret", "clients", "id_immatriculation"
        ), (
            "le LEFT JOIN d'enrichissement sur clients n'est pas un anti-join — "
            "le marquer NOT IN contredit le hint conditions (égalité requise)"
        )

    def test_enrichment_join_stays_an_equality(self):
        groups = extract_constraints(self.SQL, dialect="bigquery")
        found = False
        for g in groups:
            for a, b in g.equalities:
                pair = {(x.real_table or x.table, x.column) for x in (a, b)} | {
                    (x.table, x.column) for x in (a, b)
                }
                if {("trans", "no_siret"), ("clients", "id_immatriculation")} <= pair:
                    found = True
        assert found, "l'égalité de jointure d'enrichissement doit être préservée"

    def test_no_self_contradictory_inequality(self):
        """La paire (trans.no_siret, trans.no_siret) — issue de l'ON de l'anti-join
        lui-même après résolution — est auto-contradictoire et ne doit pas sortir."""
        groups = extract_constraints(self.SQL, dialect="bigquery")
        assert not _has_col_inequality(groups, "trans", "no_siret", "trans", "no_siret")


# ── Régression — hint anti-join actionnable (colonnes sources du critère) ─────


class TestAntiJoinSourceColumnsInHint:
    """Le hint anti-join cite le critère en termes de colonnes de CTE
    (`prop_siret_banque.groupe IN (…)`) — illisible pour le générateur qui
    remplit `banques_france.groupe`. Quand le critère porte sur une colonne
    dérivée (CASE), le hint doit nommer la colonne SOURCE et sa dérivation,
    sinon le modèle choisit une valeur interdite sans le savoir (c1.sql :
    groupe='Banque Populaire' → mappé 'BPCE' → SIRET capturé par l'anti-join
    → 0 ligne)."""

    SQL = """
    WITH banques AS (
        SELECT code_banque,
               CASE WHEN groupe IN ('BP', 'CE') THEN 'BPCE' ELSE groupe END AS groupe
        FROM `ds.banques_france`
    ),
    onus AS (
        SELECT b.code_banque
        FROM banques b
        WHERE b.groupe IN ('BPCE')
    )
    SELECT t.id
    FROM `ds.trans` t
    LEFT JOIN onus o ON t.cd_banque = o.code_banque
    WHERE o.code_banque IS NULL
    """

    def test_hint_names_source_column(self):
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        anti = " ".join(hint.get("anti_joins", []))
        assert anti, "un anti-join doit être détecté"
        assert "banques_france.groupe" in anti, (
            "le hint doit remonter à la colonne source générée "
            f"(banques_france.groupe), pas seulement au critère CTE : {anti!r}"
        )

    def test_hint_exposes_derivation(self):
        hint = build_conditions_hint(self.SQL, dialect="bigquery")
        anti = " ".join(hint.get("anti_joins", []))
        assert "CASE" in anti, (
            "la dérivation CASE doit être visible pour que le modèle évalue "
            f"le critère APRÈS transformation : {anti!r}"
        )
