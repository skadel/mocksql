"""
Juge pour l'évaluation de la première génération de tests MockSQL.

Utilise make_llm() du backend (Vertex AI via ChatGoogleGenerativeAI(vertexai=True))
si VERTEX_PROJECT est configuré — même pattern que les autres agents MockSQL.

Évalue un seul test (happy path) sur 3 critères :
  - cohérence_données  : les données injectées sont-elles plausibles ?
  - cohérence_test     : le scénario représente-t-il un cas d'usage réel ?
  - lisibilité_métier  : la description et les suggestions sont-elles accessibles à un non-technique ?
"""

import json
import os
import sys
import time
from pathlib import Path

# Rend les modules back/ importables (poetry run positionne déjà le venv back)
_BACK_DIR = Path(__file__).parent.parent.parent / "back"
if str(_BACK_DIR) not in sys.path:
    sys.path.insert(0, str(_BACK_DIR))

from utils.llm_factory import make_llm  # noqa: E402
from langchain_core.messages import HumanMessage  # noqa: E402

_PROMPT = """\
Tu es un data engineer expérimenté. Évalue la qualité du premier test généré \
automatiquement par MockSQL pour cette requête SQL.

## Requête SQL
```sql
{sql}
```

## Test généré
Statut d'exécution : {exec_status}

Description du scénario testé :
{description}

Données d'entrée injectées :
{input_data}

Résultat attendu :
{expected_output}

Assertions de validation :
{assertions}

Suggestions générées pour ce modèle :
{suggestions}

## Critères d'évaluation (note de 1 à 5)

**cohérence_données** : Les lignes injectées en entrée sont-elles plausibles \
et suffisantes pour que la requête retourne un résultat sensé ?
  - 1 = données absurdes, vides ou incompatibles avec le SQL
  - 5 = données réalistes et bien choisies pour ce cas nominal

**cohérence_test** : Le scénario testé représente-t-il bien un cas d'usage \
réel et compréhensible ?
  - 1 = scénario incompréhensible ou hors-sujet
  - 5 = cas nominal clair, qu'un ingé data reconnaît immédiatement

**lisibilité_métier** : La description du scénario, les assertions de validation \
et les suggestions sont-elles formulées dans un langage accessible à un \
utilisateur métier non-technique (responsable produit, analyste) ?
  - 1 = jargon SQL/technique pur (alias de colonnes, types, noms bruts) — \
opaque sans connaître le code
  - 3 = partiellement compréhensible, quelques termes techniques non expliqués
  - 5 = langage naturel clair et concis — un non-développeur comprend \
immédiatement ce qui est vérifié et pourquoi

Retourne un objet JSON avec exactement ces champs :
- "cohérence_données" (int 1-5)
- "cohérence_test" (int 1-5)
- "lisibilité_métier" (int 1-5)
- "reasoning" (str, 1 phrase max expliquant les notes)
- "is_valid" (bool : true si exec_status != "empty" ET cohérence_données >= 3 \
ET cohérence_test >= 3)
"""


def _format_suggestions(suggestions: list) -> str:
    if not suggestions:
        return "Aucune"
    parts = []
    for s in suggestions:
        if isinstance(s, str):
            parts.append(f"- {s}")
        elif isinstance(s, dict):
            text = s.get("text") or s.get("title") or s.get("description") or str(s)
            parts.append(f"- {text}")
    return "\n".join(parts) if parts else "Aucune"


def _format_assertions(expected_output: dict | list | str) -> str:
    if not expected_output:
        return "Aucune"
    if isinstance(expected_output, str):
        return expected_output
    if isinstance(expected_output, list):
        parts = []
        for a in expected_output:
            if isinstance(a, str):
                parts.append(f"- {a}")
            elif isinstance(a, dict):
                sql = a.get("sql") or a.get("assertion") or a.get("query") or str(a)
                parts.append(f"- {sql}")
        return "\n".join(parts) if parts else "Aucune"
    return json.dumps(expected_output, ensure_ascii=False)


def judge_first_test(
    sql: str,
    test_case: dict,
    gcp_project: str | None = None,
    model_name: str | None = None,
) -> dict:
    if gcp_project:
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", gcp_project)

    llm = make_llm(temperature=0.1, model=model_name)

    exec_status = (
        test_case.get("exec_status")
        or test_case.get("status")
        or "unknown"
    )
    description = (
        test_case.get("unit_test_description")
        or test_case.get("description")
        or test_case.get("test_name")
        or ""
    )
    # Support both server format (input_data/expected_output) and CLI format (data/assertion_results)
    input_data = json.dumps(
        test_case.get("input_data") or test_case.get("data") or {}, indent=2, ensure_ascii=False
    )
    raw_expected = test_case.get("expected_output") or test_case.get("assertion_results") or {}
    expected_output = json.dumps(raw_expected, indent=2, ensure_ascii=False)
    assertions = _format_assertions(raw_expected)
    suggestions = _format_suggestions(test_case.get("suggestions") or [])

    prompt = _PROMPT.format(
        sql=sql,
        exec_status=exec_status,
        description=description or "Non renseignée",
        input_data=input_data,
        expected_output=expected_output,
        assertions=assertions,
        suggestions=suggestions,
    )

    for attempt in range(3):
        try:
            response = llm.invoke([HumanMessage(content=prompt)])
            text = response.content
            if isinstance(text, list):
                text = " ".join(p.get("text", "") if isinstance(p, dict) else str(p) for p in text)
            # Strip markdown fences if present
            if "```" in text:
                text = text.split("```")[1].lstrip("json").strip()
            result = json.loads(text)
            result.setdefault("is_valid", False)
            return result
        except Exception as exc:
            if attempt == 2:
                return {
                    "cohérence_données": 0,
                    "cohérence_test": 0,
                    "lisibilité_métier": 0,
                    "reasoning": f"Erreur juge (tentative {attempt + 1}): {exc}",
                    "is_valid": False,
                }
            time.sleep(2)

    return {
        "cohérence_données": 0,
        "cohérence_test": 0,
        "lisibilité_métier": 0,
        "reasoning": "Echec",
        "is_valid": False,
    }
