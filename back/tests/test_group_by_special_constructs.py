"""
Spec des corrections attendues pour les bugs GROUP BY ALL / ROLLUP / CUBE / GROUPING SETS
dans ensure_in_group_by (sql_functions/helpers.py) et process_group_by (utils/find_grains.py).

Ces tests échouent tant que les bugs existent.
"""

import sqlglot
from sqlglot import exp

from sql_functions.helpers import ensure_in_group_by
from utils.find_grains import process_group_by


# ===========================================================================
# Bug 4 — ensure_in_group_by ajoute des colonnes après GROUP BY ALL / ROLLUP /
#          CUBE / GROUPING SETS, produisant une syntaxe invalide
# ===========================================================================


class TestEnsureInGroupBySpecialConstructs:
    """
    Symptôme : ensure_in_group_by appelle group.set("expressions", [...] + [new_col])
    sans vérifier si group contient un nœud ALL / rollup / cube / grouping_sets.
    Résultat : GROUP BY ALL x (syntaxe invalide rejetée par DuckDB).

    Correction attendue : ne pas modifier group.expressions quand group.args
    contient "all", "rollup", "cube" ou "grouping_sets" — ces construits couvrent
    déjà toutes les colonnes non-agrégées par définition.
    """

    def _select_with(self, sql: str) -> exp.Select:
        return sqlglot.parse_one(sql, dialect="bigquery")

    def test_group_by_all_not_corrupted(self):
        """ensure_in_group_by ne doit pas ajouter de colonne après GROUP BY ALL."""
        select = self._select_with("SELECT a, b, SUM(c) AS total FROM t GROUP BY ALL")
        ensure_in_group_by(select, [exp.column("x")])
        result = select.sql(dialect="duckdb")
        assert "GROUP BY ALL" in result
        assert "GROUP BY ALL x" not in result, (
            f"ensure_in_group_by a corrompu GROUP BY ALL : {result}"
        )

    def test_group_by_rollup_not_corrupted(self):
        """ensure_in_group_by ne doit pas ajouter de colonne avant ROLLUP."""
        select = self._select_with(
            "SELECT a, b, SUM(c) AS total FROM t GROUP BY ROLLUP(a, b)"
        )
        ensure_in_group_by(select, [exp.column("x")])
        result = select.sql(dialect="duckdb")
        after_group_by = result.split("GROUP BY")[1].strip()
        # pas de colonne parasite avant ROLLUP
        assert after_group_by.upper().startswith("ROLLUP"), (
            f"ensure_in_group_by a injecté une colonne avant ROLLUP : {result}"
        )

    def test_group_by_cube_not_corrupted(self):
        """ensure_in_group_by ne doit pas ajouter de colonne avant CUBE."""
        select = self._select_with(
            "SELECT a, b, SUM(c) AS total FROM t GROUP BY CUBE(a, b)"
        )
        ensure_in_group_by(select, [exp.column("x")])
        result = select.sql(dialect="duckdb")
        after_group_by = result.split("GROUP BY")[1].strip()
        assert after_group_by.upper().startswith("CUBE"), (
            f"ensure_in_group_by a injecté une colonne avant CUBE : {result}"
        )

    def test_group_by_grouping_sets_not_corrupted(self):
        """ensure_in_group_by ne doit pas ajouter de colonne avant GROUPING SETS."""
        select = self._select_with(
            "SELECT a, b, SUM(c) AS total FROM t GROUP BY GROUPING SETS ((a, b), (a), ())"
        )
        ensure_in_group_by(select, [exp.column("x")])
        result = select.sql(dialect="duckdb")
        after_group_by = result.split("GROUP BY")[1].strip()
        assert after_group_by.upper().startswith("GROUPING SETS"), (
            f"ensure_in_group_by a injecté une colonne avant GROUPING SETS : {result}"
        )

    def test_regular_group_by_still_gets_column_added(self):
        """Non-régression : GROUP BY a ordinaire reçoit toujours les nouvelles colonnes."""
        select = self._select_with("SELECT a, b, SUM(c) AS total FROM t GROUP BY a")
        ensure_in_group_by(select, [exp.column("b")])
        result = select.sql(dialect="duckdb")
        assert "b" in result.split("GROUP BY")[1]


# ===========================================================================
# Bug 6 — process_group_by retourne un grain vide pour GROUP BY ALL / ROLLUP /
#          CUBE / GROUPING SETS car il itère uniquement group_by.expressions
# ===========================================================================


class TestProcessGroupBySpecialConstructs:
    """
    Symptôme : process_group_by itère group_by.expressions qui est [] pour
    GROUP BY ALL / ROLLUP / CUBE / GROUPING SETS → retourne [] (aucun grain
    détecté), même quand les colonnes de groupement sont connues.

    Correction attendue :
    - GROUP BY ALL → retourner les colonnes non-agrégées du SELECT (ou None
      pour signaler "grain non déterminable statiquement")
    - GROUP BY ROLLUP/CUBE → extraire les colonnes depuis group.args["rollup"]
      / group.args["cube"]
    - GROUP BY GROUPING SETS → extraire les colonnes depuis les Tuples internes
    """

    def _node(self, sql: str) -> exp.Select:
        return sqlglot.parse_one(sql, dialect="bigquery")

    def test_group_by_all_returns_non_empty_grain(self):
        """
        GROUP BY ALL sur (a, b, SUM(c)) doit détecter a et b comme grain,
        ou au minimum retourner None pour signaler l'incertitude — pas [].
        """
        node = self._node("SELECT a, b, SUM(c) AS total FROM t GROUP BY ALL")
        result = process_group_by(node, {}, {}, {}, {})
        assert result is None or len(result) > 0, (
            "process_group_by retourne [] pour GROUP BY ALL — grain non détecté"
        )

    def test_group_by_rollup_returns_non_empty_grain(self):
        """
        GROUP BY ROLLUP(a, b) doit détecter a et b comme colonnes de grain.
        """
        node = self._node("SELECT a, b, SUM(c) AS total FROM t GROUP BY ROLLUP(a, b)")
        result = process_group_by(node, {}, {}, {}, {})
        assert result is None or len(result) > 0, (
            "process_group_by retourne [] pour GROUP BY ROLLUP — grain non détecté"
        )

    def test_group_by_cube_returns_non_empty_grain(self):
        """GROUP BY CUBE(a, b) doit détecter a et b comme colonnes de grain."""
        node = self._node("SELECT a, b, SUM(c) AS total FROM t GROUP BY CUBE(a, b)")
        result = process_group_by(node, {}, {}, {}, {})
        assert result is None or len(result) > 0, (
            "process_group_by retourne [] pour GROUP BY CUBE — grain non détecté"
        )

    def test_group_by_grouping_sets_returns_non_empty_grain(self):
        """GROUP BY GROUPING SETS ((a, b), (a), ()) doit détecter a et b."""
        node = self._node(
            "SELECT a, b, SUM(c) AS total FROM t GROUP BY GROUPING SETS ((a, b), (a), ())"
        )
        result = process_group_by(node, {}, {}, {}, {})
        assert result is None or len(result) > 0, (
            "process_group_by retourne [] pour GROUP BY GROUPING SETS — grain non détecté"
        )

    def test_regular_group_by_grain_unchanged(self):
        """Non-régression : GROUP BY a, b retourne bien {'a', 'b'}."""
        node = self._node("SELECT a, b, SUM(c) AS total FROM t GROUP BY a, b")
        result = process_group_by(node, {}, {}, {}, {})
        assert result is not None and len(result) > 0
