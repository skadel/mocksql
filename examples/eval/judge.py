"""
Juge Gemini pour l'évaluation de la première génération de tests MockSQL.

Évalue un seul test (happy path) sur 2 critères :
  - cohérence_données : les données injectées sont-elles plausibles ?
  - cohérence_test    : le scénario représente-t-il un cas d'usage réel ?
"""

import json
import os
import time

from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.messages import HumanMessage

_PROMPT = """\
Tu es un data engineer expérimenté. Évalue la qualité du premier test généré \
automatiquement par MockSQL pour cette requête SQL.

## Requête SQL
```sql
{sql}
```

## Test généré
Statut d'exécution : {exec_status}

Données d'entrée injectées :
{input_data}

Résultat attendu :
{expected_output}

## Critères d'évaluation (note de 1 à 5)

**cohérence_données** : Les lignes injectées en entrée sont-elles plausibles \
et suffisantes pour que la requête retourne un résultat sensé ?
  - 1 = données absurdes, vides ou incompatibles avec le SQL
  - 5 = données réalistes et bien choisies pour ce cas nominal

**cohérence_test** : Le scénario testé représente-t-il bien un cas d'usage \
réel et compréhensible ?
  - 1 = scénario incompréhensible ou hors-sujet
  - 5 = cas nominal clair, qu'un ingé data reconnaît immédiatement

Retourne un objet JSON avec exactement ces champs :
- "cohérence_données" (int 1-5)
- "cohérence_test" (int 1-5)
- "reasoning" (str, 1 phrase max expliquant les notes)
- "is_valid" (bool : true si exec_status != "empty" ET cohérence_données >= 3 \
ET cohérence_test >= 3)
"""


def judge_first_test(
    sql: str,
    test_case: dict,
    gcp_project: str | None = None,
    location: str = "us-central1",
    model_name: str = "gemini-2.0-flash",
) -> dict:
    if gcp_project:
        os.environ.setdefault("GOOGLE_CLOUD_PROJECT", gcp_project)

    llm = ChatGoogleGenerativeAI(
        model=model_name,
        vertexai=True,
        temperature=0.1,
    )

    exec_status = (
        test_case.get("exec_status")
        or test_case.get("status")
        or "unknown"
    )
    # Support both server format (input_data/expected_output) and CLI format (data/assertion_results)
    input_data = json.dumps(
        test_case.get("input_data") or test_case.get("data") or {}, indent=2, ensure_ascii=False
    )
    expected_output = json.dumps(
        test_case.get("expected_output") or test_case.get("assertion_results") or {}, indent=2, ensure_ascii=False
    )

    prompt = _PROMPT.format(
        sql=sql,
        exec_status=exec_status,
        input_data=input_data,
        expected_output=expected_output,
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
                    "reasoning": f"Erreur juge (tentative {attempt + 1}): {exc}",
                    "is_valid": False,
                }
            time.sleep(2)

    return {"cohérence_données": 0, "cohérence_test": 0, "reasoning": "Echec", "is_valid": False}
