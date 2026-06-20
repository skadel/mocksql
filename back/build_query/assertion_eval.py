"""Évaluation d'assertions dbt-style contre DuckDB — pur SQL, zéro dépendance LLM.

Isolé de ``examples_executor`` (qui tire langchain/langgraph au import) pour que le
replay CI (``mocksql test`` / ``cli/test_runner.py``) — qui ne fait AUCUN appel LLM —
ne paie pas ~3-4s de chargement du stack LLM. ``examples_executor`` ré-exporte
``_evaluate_assertions`` depuis ici pour ses appelants internes.
"""

from __future__ import annotations

from typing import Any, Dict, List


def _evaluate_assertions(
    assertions: List[Dict[str, Any]], view_name: str, con
) -> List[Dict[str, Any]]:
    """
    Évalue chaque assertion dbt-style contre le DataFrame résultat enregistré sous view_name.

    Convention dbt-style : une assertion SQL doit retourner les lignes ÉCHOUANTES.
      - 0 ligne retournée → assertion passée (passed=True)
      - ≥1 ligne retournée → assertion échouée (passed=False), les lignes sont des contre-exemples

    Exemple : pour vérifier que `start_station_name` vaut toujours 'Central Park' :
      SELECT * FROM __result__ WHERE start_station_name != 'Central Park'
      → retourne les lignes où la station est incorrecte ; 0 ligne = OK.

    Ne pas confondre avec une assertion positive (WHERE col = 'X') qui retournerait
    des lignes quand la condition est vraie — ce serait l'inverse de la convention.
    """
    results = []
    for a in assertions:
        raw_sql = (a.get("sql") or "").strip()
        if not raw_sql:
            # SQL vide → con.execute("") renvoie None → ".fetchdf()" planterait avec un
            # message opaque ("'NoneType' object has no attribute 'fetchdf'"). On émet une
            # erreur explicite à la place (assertion malformée, ex. dérivation `sql` oubliée).
            results.append(
                {
                    "description": a.get("description", ""),
                    "expected_condition": a.get("expected_condition", ""),
                    "sql": a.get("sql", ""),
                    "passed": False,
                    "error": "assertion SQL vide (aucune requête à exécuter)",
                }
            )
            continue
        sql = raw_sql.replace("__result__", view_name)
        scope = (a.get("scope") or "").strip().rstrip(";").strip()
        try:
            # Garde anti-vacuité du scope : une assertion scopée dont le périmètre ne
            # sélectionne AUCUNE ligne du résultat ne teste rien (0 ligne violante →
            # « passe » à tort). On l'échoue explicitement plutôt que de la laisser verte.
            if scope:
                scope_sql = scope.replace("__result__", view_name)
                covered = con.execute(
                    f"SELECT COUNT(*) FROM {view_name} WHERE ({scope_sql})"
                ).fetchone()[0]
                if covered == 0:
                    results.append(
                        {
                            "description": a.get("description", ""),
                            "expected_condition": a.get("expected_condition", ""),
                            "scope": a.get("scope", ""),
                            "sql": a.get("sql", ""),
                            "passed": False,
                            "error": (
                                "le périmètre (scope) ne sélectionne aucune ligne du "
                                "résultat — l'assertion ne teste rien (vacante)"
                            ),
                        }
                    )
                    continue
            fail_df = con.execute(sql).fetchdf()
            passed = len(fail_df) == 0
            results.append(
                {
                    "description": a.get("description", ""),
                    "expected_condition": a.get("expected_condition", ""),
                    **({"scope": a.get("scope", "")} if scope else {}),
                    "sql": a.get("sql", ""),
                    "passed": passed,
                    "failing_rows": fail_df.to_dict(orient="records")
                    if not passed
                    else [],
                }
            )
        except Exception as e:
            results.append(
                {
                    "description": a.get("description", ""),
                    "expected_condition": a.get("expected_condition", ""),
                    "sql": a.get("sql", ""),
                    "passed": False,
                    "error": str(e),
                }
            )
    return results
