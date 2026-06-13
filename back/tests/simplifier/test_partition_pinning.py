"""
Régression — épinglage de partition (« partition pinning »).

Pattern BigQuery omniprésent :

    WHERE partition_date = (SELECT MAX(partition_date) FROM t
                            WHERE partition_date <= <date>)

Avant le fix, `_dispatch_pred` ignorait silencieusement tout prédicat
`col = (SELECT …)` : la colonne paraissait « non contrainte », sortait du
schéma Pydantic du générateur (le LLM ne pouvait pas la produire) et partait
en remplissage aléatoire via le sparse_filler — la CTE filtrée devenait vide
et la requête retournait 0 ligne (cf. examples/spider_complexified/models/c1.sql).

Comportement attendu :
  1. La colonne externe ET la colonne interne du MAX sont marquées contraintes
     (présentes dans source_columns / equivalence_classes).
  2. Cas cross-table (`banques.partition_date = MAX(banques_france.partition_date)`) :
     une classe d'équivalence relie les deux colonnes — générer la même date
     des deux côtés rend le MAX trivialement égal.
  3. Le WHERE interne (`partition_date <= <date>`) devient une contrainte de
     génération conservatrice (op lte) sur la colonne interne.
  4. Un CTE dont le SEUL prédicat est l'épinglage propage quand même ses
     contraintes au groupe externe (le merge ne doit pas le jeter).
"""

from pathlib import Path

import pytest

from build_query.constraint_simplifier import simplify


# ─── Helpers ─────────────────────────────────────────────────────────────────


def _tbl_match(ref, name: str) -> bool:
    return ref.table == name or ref.real_table == name


def _constrained(result) -> set[tuple[str, str]]:
    """Réplique le calcul du set `constrained` de _compute_faker_columns :
    toute colonne absente de ce set est remplie aléatoirement par le sparse_filler."""
    out: set[tuple[str, str]] = set()
    for ref in result.source_columns:
        out.add((ref.table.lower(), ref.column.lower()))
    for ref in result.derived_columns:
        out.add((ref.table.lower(), ref.column.lower()))
    for eq_class in result.equivalence_classes:
        for ref in eq_class:
            out.add((ref.table.lower(), ref.column.lower()))
    for f in result.filters:
        for src in f.source_columns:
            out.add((src.table.lower(), src.column.lower()))
    return out


def _is_constrained(result, table: str, column: str) -> bool:
    return (table, column) in _constrained(result)


def _filter_ops_on(result, table: str, column: str) -> list[str]:
    return [
        f.op
        for f in result.filters
        if _tbl_match(f.column, table) and f.column.column == column
    ]


def _same_class(result, *cols: tuple[str, str]) -> bool:
    wanted = set(cols)
    for cls in result.equivalence_classes:
        got = {(r.real_table or r.table, r.column) for r in cls} | {
            (r.table, r.column) for r in cls
        }
        if wanted <= got:
            return True
    return False


# ─── 1. Épinglage self-table (pattern COFACE) ────────────────────────────────


class TestSelfTablePinning:
    SQL = """
    SELECT cosirt, copost
    FROM `ds.coface`
    WHERE partition_date = (
        SELECT MAX(partition_date)
        FROM `ds.coface`
        WHERE partition_date <= PARSE_DATE('%d-%m-%Y', '01-01-2026')
    )
    """

    def test_partition_date_is_constrained(self):
        result = simplify(self.SQL, dialect="bigquery")
        assert _is_constrained(result, "coface", "partition_date"), (
            "partition_date doit être marquée contrainte — sinon elle est "
            "remplie aléatoirement par le sparse_filler et la CTE devient vide"
        )

    def test_inner_bound_becomes_lte_filter(self):
        result = simplify(self.SQL, dialect="bigquery")
        ops = _filter_ops_on(result, "coface", "partition_date")
        assert "lte" in ops, f"attendu un filtre lte (borne du MAX), ops={ops}"


# ─── 2. Épinglage cross-table (pattern RESEAU de c1.sql) ────────────────────


class TestCrossTablePinning:
    SQL = """
    SELECT code_banque, reseau
    FROM `ds.banques`
    WHERE reseau IN ('BP', 'CE')
      AND partition_date = (
        SELECT MAX(partition_date)
        FROM `ds.banques_france`
        WHERE partition_date <= PARSE_DATE('%d-%m-%Y', '01-01-2026')
    )
    """

    def test_both_columns_constrained(self):
        result = simplify(self.SQL, dialect="bigquery")
        assert _is_constrained(result, "banques", "partition_date")
        assert _is_constrained(result, "banques_france", "partition_date")

    def test_equivalence_class_links_outer_and_inner(self):
        result = simplify(self.SQL, dialect="bigquery")
        assert _same_class(
            result,
            ("banques", "partition_date"),
            ("banques_france", "partition_date"),
        ), (
            "générer la même date des deux côtés est la seule stratégie sûre : "
            "le MAX devient trivialement égal à la valeur épinglée"
        )

    def test_inner_bound_becomes_lte_filter(self):
        result = simplify(self.SQL, dialect="bigquery")
        ops = _filter_ops_on(result, "banques_france", "partition_date")
        assert "lte" in ops, f"attendu un filtre lte sur la table interne, ops={ops}"


# ─── 3. CTE dont le seul prédicat est l'épinglage ────────────────────────────


class TestPinningOnlyCtePropagates:
    SQL = """
    WITH coface AS (
        SELECT *
        FROM `ds.coface_raw`
        WHERE partition_date = (
            SELECT MAX(partition_date)
            FROM `ds.coface_raw`
            WHERE partition_date <= PARSE_DATE('%d-%m-%Y', '01-01-2026')
        )
    )
    SELECT o.siret, c.copost
    FROM `ds.orders` o
    JOIN coface c ON o.siret = c.cosirt
    """

    def test_cte_pinning_reaches_top_level(self):
        result = simplify(self.SQL, dialect="bigquery")
        assert _is_constrained(result, "coface_raw", "partition_date"), (
            "le merge CTE ne doit pas jeter un groupe dont le seul prédicat "
            "est l'épinglage de partition"
        )


# ─── 4. Intégration — c1.sql réel ────────────────────────────────────────────


_C1_PATH = (
    Path(__file__).resolve().parents[3]
    / "examples"
    / "spider_complexified"
    / "models"
    / "c1.sql"
)


@pytest.mark.skipif(not _C1_PATH.exists(), reason="c1.sql absent")
class TestC1PartitionDates:
    def test_all_partition_date_columns_constrained(self):
        result = simplify(_C1_PATH.read_text(encoding="utf-8"), dialect="bigquery")
        constrained = _constrained(result)
        missing = [
            table
            for table in (
                "banques_france",
                "coface",
                "naf2",
                "categories_juridiques",
                "code_mcc",
            )
            if (table, "partition_date") not in constrained
        ]
        assert not missing, (
            f"partition_date non contrainte pour {missing} — ces colonnes "
            "partiraient en remplissage aléatoire et videraient les CTEs"
        )
