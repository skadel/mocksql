"""
Juge pour l'évaluation de la première génération de tests MockSQL.

Utilise make_llm() du backend (Vertex AI via ChatGoogleGenerativeAI(vertexai=True))
si VERTEX_PROJECT est configuré — même pattern que les autres agents MockSQL.

Évalue un seul test (happy path) sur 3 critères :
  - cohérence_données  : les données sont-elles cohérentes avec le SQL ET la description ? (pas leur réalisme)
  - cohérence_test     : le scénario testé est-il clair et cohérent ?
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

## Résultat réel produit par DuckDB
{real_result}

⚠️ Ce résultat fait foi : c'est la sortie réelle de la requête sur les données injectées.
Ne suppose JAMAIS qu'une ligne est exclue par un filtre — si elle apparaît ci-dessus, elle a
passé tous les filtres de la requête. Juge le caractère vide / non-vide uniquement d'après ce bloc.

## Assertions de validation
Convention : chaque assertion exprime une **vérité positive** qui doit tenir sur chaque ligne du
résultat (ex. « le district vaut 5 »). MockSQL l'exécute en SQL dbt-style (`WHERE (cond) IS NOT TRUE`,
qui remonte les lignes VIOLANTES — 0 ligne = OK) : ce n'est PAS une logique inversée, c'est le
mécanisme de vérification standard. Juge la condition positive ci-dessous, pas une forme SQL négative.

{assertions}

Suggestions générées pour ce modèle :
{suggestions}

## Critères d'évaluation (note de 1 à 5)

**cohérence_données** : Les lignes injectées sont-elles COHÉRENTES avec (a) la logique \
SQL — elles produisent un résultat sensé, non vide pour ce scénario — ET (b) ce que la \
DESCRIPTION du test annonce ?
  ⚠️ Ce sont des données SYNTHÉTIQUES de test, PAS des données de production : leur \
RÉALISME vis-à-vis du monde réel n'entre PAS en compte. Une population de 3937 pour la \
France est parfaitement acceptable si elle est cohérente avec le SQL et la description.
  - 1 = données incompatibles avec le SQL (résultat vide/cassé), OU qui CONTREDISENT la \
description (la description annonce 1000 cas et une population de 67M, mais on injecte \
8697 cas et une population de 3937)
  - 5 = données qui font tourner la requête comme prévu ET fidèles à ce que la description \
annonce (mêmes valeurs d'entrée, même cas) — qu'elles soient réalistes ou non

**cohérence_test** : Le scénario testé est-il clair, cohérent et bien ciblé (la \
description, les données et les assertions racontent la même histoire) ?
  - 1 = scénario incompréhensible, hors-sujet, ou incohérent en interne
  - 5 = cas nominal clair et cohérent de bout en bout, qu'un ingé data reconnaît immédiatement

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
- "is_valid" (bool : true si le résultat réel produit par DuckDB n'est PAS vide \
ET cohérence_données >= 3 ET cohérence_test >= 3)
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
    """Rend chaque assertion sous sa forme métier (description + condition POSITIVE attendue),
    pas le SQL brut. Le SQL stocké est dbt-style (`WHERE (cond) IS NOT TRUE` = remonte les
    lignes violantes) ; le montrer tel quel induisait le juge en erreur (« logique inversée »).
    On expose la vérité positive que le test affirme — ce que voit l'utilisateur dans l'UI."""
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
                desc = (a.get("description") or "").strip()
                cond = (a.get("expected_condition") or "").strip()
                if desc and cond:
                    label = f"{desc} (condition attendue, vraie sur chaque ligne : `{cond}`)"
                elif desc:
                    label = desc
                elif cond:
                    label = f"condition attendue : `{cond}`"
                else:
                    label = a.get("sql") or a.get("assertion") or a.get("query") or str(a)
                passed = a.get("passed")
                if passed is True:
                    label += " → vérifiée ✓"
                elif passed is False:
                    label += " → NON vérifiée ✗"
                parts.append(f"- {label}")
        return "\n".join(parts) if parts else "Aucune"
    return json.dumps(expected_output, ensure_ascii=False)


def _format_real_result(test_case: dict) -> str:
    """Rend le résultat réel produit par DuckDB (la vérité d'exécution), pour que le juge
    n'ait PAS à simuler mentalement la requête — source des hallucinations de « résultat vide »
    (ex. bq282 : le juge croyait une ligne exclue par un filtre alors qu'elle était présente)."""
    raw = test_case.get("results_json")
    if raw is None:
        raw = test_case.get("results") or test_case.get("real_res")
    if raw is None:
        return "(non disponible)"
    if isinstance(raw, str):
        try:
            rows = json.loads(raw)
        except Exception:
            return raw
    else:
        rows = raw
    if not isinstance(rows, list):
        return json.dumps(rows, ensure_ascii=False)
    if len(rows) == 0:
        return "0 ligne — résultat VIDE."
    shown = rows[:10]
    body = json.dumps(shown, ensure_ascii=False, indent=2)
    suffix = "" if len(rows) <= 10 else f"\n… (+{len(rows) - 10} ligne(s) non affichée(s))"
    return f"{len(rows)} ligne(s) :\n{body}{suffix}"


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
    assertions = _format_assertions(raw_expected)
    real_result = _format_real_result(test_case)
    suggestions = _format_suggestions(test_case.get("suggestions") or [])

    prompt = _PROMPT.format(
        sql=sql,
        exec_status=exec_status,
        description=description or "Non renseignée",
        input_data=input_data,
        real_result=real_result,
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
