"""Régression root-cause spider2-snow — un INSERT rejeté par DuckDB ne doit JAMAIS
être avalé.

Incident : `execute_queries` loggait l'exception (`Conversion Error: Could not convert
string "M001" to DECIMAL(38,9)`) sans la relancer → tables vides → SELECT à 0 ligne →
misclassification `empty_results` avec un diagnostic CTE mensonger → boucle de
correction aveugle qui ne converge jamais (12+ modèles morts-nés sur l'éval).

Le contrat corrigé : toutes les requêtes sont tentées (diagnostic complet dans les
logs), puis la PREMIÈRE exception est relancée telle quelle — son message DuckDB
verbatim rend le circuit `bad_data_error` de l'executor atteignable
(`_is_duckdb_data_error` classe sur le préfixe du message).
"""

import duckdb
import pytest

from build_query.examples_executor import _is_duckdb_data_error
from utils.examples import execute_queries


def _con():
    return duckdb.connect(":memory:")


def test_insert_failure_raises_with_duckdb_message_intact():
    con = _con()
    con.execute("CREATE TABLE t (id DECIMAL(38,9))")
    with pytest.raises(Exception) as exc_info:
        execute_queries(["INSERT INTO t VALUES ('M001')"], con)
    # Le message d'origine (colonne/valeur) survit : c'est LE diagnostic que
    # l'évaluateur transmet ensuite au correcteur.
    assert "M001" in str(exc_info.value)
    # Et il est classé « erreur de données » par l'executor → circuit bad_data_error.
    assert _is_duckdb_data_error(exc_info.value)


def test_all_queries_attempted_before_raising():
    """Les requêtes suivantes sont quand même tentées : plusieurs tables fautives
    apparaissent dans les logs d'un seul coup (un retry, pas N)."""
    con = _con()
    con.execute("CREATE TABLE a (id DECIMAL(38,9))")
    con.execute("CREATE TABLE b (id DECIMAL(38,9))")
    with pytest.raises(Exception):
        execute_queries(
            ["INSERT INTO a VALUES ('bad')", "INSERT INTO b VALUES (1)"], con
        )
    assert con.execute("SELECT COUNT(*) FROM b").fetchone()[0] == 1


def test_success_path_unchanged():
    con = _con()
    con.execute("CREATE TABLE t (id INT)")
    execute_queries(["INSERT INTO t VALUES (1)", "INSERT INTO t VALUES (2)"], con)
    assert con.execute("SELECT COUNT(*) FROM t").fetchone()[0] == 2
