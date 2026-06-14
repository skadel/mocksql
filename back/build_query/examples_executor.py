import asyncio
import json
import logging
import re
import uuid
from typing import List, Dict, Any, Literal, Optional

import sqlglot
from langchain_core.messages import AIMessage, HumanMessage, SystemMessage
from pandas import DataFrame
from pydantic import BaseModel, Field, model_validator
from sqlglot import exp
from sqlglot.optimizer.simplify import simplify

from utils.llm_errors import normalize_llm_content, loads_lenient_json
from utils.llm_factory import make_llm
from utils.msg_types import MsgType
from build_query.state import QueryState
from utils.examples import (
    run_query_on_test_dataset,
    create_test_tables,
    execute_queries,
    initialize_duckdb,
    DB_PATH,
)
from utils.insert_examples import replace_missing_with_null, insert_examples
from storage.test_repository import get_test
from utils.saver import examples_state_retriever


import utils.logger  # noqa: F401 — registers DIAG level (15)

logger = logging.getLogger(__name__)


def _assertion_sql_from_condition(
    expected_condition: str, scope: Optional[str] = None
) -> str:
    """Wrappe une condition positive (vérité métier qui doit tenir sur chaque ligne)
    en requête dbt-style retournant les lignes VIOLANTES (0 ligne = OK).

    Le LLM exprime l'affirmation attendue (ex. ``date = '2016-01-02'``) ; la négation
    est gérée ici, mécaniquement — le LLM n'écrit donc jamais d'assertion inversée
    (``!=`` / ``NOT``), ce qui supprime les inversions par erreur et garde la
    description lisible comme une affirmation.

    On utilise ``IS NOT TRUE`` (et non ``NOT (...)``) pour que les NULL comptent comme
    violations : ``NOT(NULL)`` vaut NULL et laisserait passer un NULL là où une valeur
    est attendue, alors que ``(NULL) IS NOT TRUE`` est vrai → la ligne est remontée.

    ``scope`` (optionnel) restreint l'univers : la condition n'est testée QUE sur les
    lignes que ``scope`` sélectionne. Indispensable pour affirmer un fait sur UNE ligne
    précise d'un résultat MULTI-lignes (ex. « la ligne au montant le plus ancien est X »)
    sans que les autres lignes ne soient comptées comme violantes. Reste POSITIF :
    ``scope = "d = (SELECT MIN(d) FROM __result__)"`` + ``condition = "id = 'X'"`` plutôt
    que la forme ``id = 'X' AND d = (SELECT MIN(d) …)`` qui échoue à tort sur toute autre
    ligne. La couverture du scope (≥1 ligne) est vérifiée à l'exécution — un scope vide
    rend l'assertion vacuité et la fait échouer (cf. ``_evaluate_assertions``).
    """
    cond = expected_condition.strip().rstrip(";").strip()
    sc = (scope or "").strip().rstrip(";").strip()
    if sc:
        return f"SELECT * FROM __result__ WHERE ({sc}) AND (({cond}) IS NOT TRUE)"
    return f"SELECT * FROM __result__ WHERE ({cond}) IS NOT TRUE"


def _has_negative_form(expr: exp.Expression) -> bool:
    """Vrai si l'AST contient une forme négative détournée (« vérifie ce qui ne doit PAS
    être là ») — mêmes interdits que la consigne du champ ``_Assertion.expected_condition`` :
    ``!=`` / ``<>`` (et ``IS DISTINCT FROM``), ``NOT IN``, ``NOT (...)``, ``NOT LIKE``,
    ``IS NULL``.

    On inspecte l'arbre sqlglot plutôt qu'une regex pour ne pas se faire piéger par un
    littéral chaîne (``status = 'is null'`` n'est PAS une clause ``IS NULL``).

    Seule négation tolérée : ``X IS NOT NULL`` — une affirmation de présence, donc une
    forme positive légitime (que l'ancienne garde regex autorisait déjà).
    """
    for node in expr.walk():
        # `!=` / `<>` et son équivalent NULL-safe `X IS DISTINCT FROM Y` (= `!=`).
        if isinstance(node, (exp.NEQ, exp.NullSafeNEQ)):
            return True
        if isinstance(node, exp.Not):
            inner = node.this
            # `X IS NOT NULL` = Not(Is(..., Null)) sans parenthèses → toléré.
            if isinstance(inner, exp.Is) and isinstance(inner.expression, exp.Null):
                continue
            return True
        if isinstance(node, exp.Is) and isinstance(node.expression, exp.Null):
            # `X IS NULL` nu interdit ; l'`Is` interne d'un `IS NOT NULL` est sous un
            # `Not` déjà toléré ci-dessus → ne pas le re-flaguer.
            if not isinstance(node.parent, exp.Not):
                return True
    return False


# Comparaisons dont des opérandes identiques rendent l'assertion vacuité : `x = x` /
# `x >= x` / `x <= x` toujours vraies (ne signalent jamais rien), `x > x` / `x < x` toujours
# fausses (`(faux) IS NOT TRUE` toujours vrai → 0 ligne violante → « passe » sans tester).
_SAME_OPERAND_COMPARISONS = (exp.EQ, exp.GT, exp.LT, exp.GTE, exp.LTE)

# Sous-ensemble TOUJOURS-VRAI (et non toujours-faux) : sert à la propagation AND/OR, où
# seul un opérande toujours-vrai compte (`x > x` toujours-faux n'aide pas à rendre un OR vrai).
_ALWAYS_TRUE_SAME_OPERAND = (exp.EQ, exp.GTE, exp.LTE)


def _is_always_true(expr: exp.Expression) -> bool:
    """Vrai si ``expr`` est TOUJOURS vraie (tautologie stricte). Distinct du test
    « constante booléenne » de ``_is_trivial_tautology`` qui rejette aussi les contradictions
    (toujours-fausses) : pour propager via ``OR`` (``FALSE OR x`` ≡ ``x``, non vacuité) il faut
    pouvoir dire qu'un opérande est vrai, pas seulement constant.

    Couvre la constante ``TRUE`` après ``simplify``, les comparaisons same-operand toujours-vraies
    (``x = x`` / ``x >= x`` / ``x <= x``), et la propagation : ``AND`` vrai ssi TOUS ses opérandes
    le sont, ``OR`` vrai ssi AU MOINS UN l'est (sqlglot imbrique ``a AND b AND c`` en
    ``And(And(a, b), c)`` → la récursion couvre les arités > 2).
    """
    node = expr.unnest()
    try:
        s = simplify(node.copy())
        if isinstance(s, exp.Boolean):
            return bool(s.this)  # TRUE seulement, pas FALSE
    except Exception:
        pass
    if isinstance(node, _ALWAYS_TRUE_SAME_OPERAND) and node.this == node.expression:
        return True
    if isinstance(node, exp.And):
        return _is_always_true(node.this) and _is_always_true(node.expression)
    if isinstance(node, exp.Or):
        return _is_always_true(node.this) or _is_always_true(node.expression)
    return False


def _is_trivial_tautology(expr: exp.Expression) -> bool:
    """Vrai si la condition ne contraint rien : elle « passe » quelles que soient les
    données. Familles couvertes :

    - constante booléenne après ``simplify`` (``1 = 1``, ``TRUE``, ``1 < 2``…) ;
    - comparaison de tête à opérandes structurellement identiques (``x = x``, ``x >= x``,
      ``lower(c) = lower(c)``) ;
    - composé ``AND`` / ``OR`` toujours-vrai par propagation (``x = x AND y = y``,
      ``x = x OR y > 5``) — délégué à ``_is_always_true``.

    C'est la seule classe de vacuité que la ré-exécution (Garde 2) laisse passer : une
    tautologie « passe » sur les données réelles ET sur n'importe quelles données, donc
    `_evaluate_assertions` la valide à tort. (Les composés toujours-FAUX, eux, échouent
    bruyamment et sont rejetés par la ré-exécution.)
    """
    try:
        if isinstance(simplify(expr.copy()), exp.Boolean):
            return True
    except Exception:
        pass
    node = expr.unnest()
    if isinstance(node, _SAME_OPERAND_COMPARISONS) and node.this == node.expression:
        return True
    if isinstance(node, (exp.And, exp.Or)):
        return _is_always_true(node)
    return False


def _is_valid_positive_condition(cond: str) -> bool:
    """Vrai si ``cond`` est une condition booléenne POSITIVE exploitable par
    ``_assertion_sql_from_condition`` : non vide, parsable, expression booléenne (pas une
    requête ``SELECT``/``WHERE``), sans forme négative détournée (cf. ``_has_negative_form``)
    et non tautologique (cf. ``_is_trivial_tautology``).

    Garde anti-blanchiment du fixer d'assertions : empêche de remplacer une assertion
    échouée par du SQL libre auto-contradictoire (ex. ``x = 2 AND (SELECT COUNT(*) … ) = 0``)
    qui « passe » sans rien tester. Une condition positive enveloppée dans ``IS NOT TRUE``
    ne peut jamais être vacuité : si aucune ligne ne la satisfait, l'assertion échoue
    bruyamment au lieu de passer.

    Le filtrage passe par l'AST sqlglot (et non une regex) : un littéral chaîne contenant
    ``is null`` / ``not in`` reste une condition positive valide, et une sous-requête
    relative (``z = (SELECT MAX(z) …)``) n'est pas confondue avec une requête de tête.
    """
    c = cond.strip().rstrip(";").strip()
    if not c:
        return False
    try:
        parsed = sqlglot.parse_one(c, dialect="duckdb")
    except Exception:
        # Non parsable → on ne sait pas la maîtriser : rejet (on garde l'assertion
        # d'origine en échec plutôt que d'injecter une forme inconnue).
        return False
    # On attend une expression booléenne, pas une requête complète.
    if isinstance(parsed, exp.Select):
        return False
    if _is_trivial_tautology(parsed):
        return False
    return not _has_negative_form(parsed)


class _Assertion(BaseModel):
    description: str = Field(
        description=(
            "Phrase EN FRANÇAIS, courte (max 12 mots), décrivant l'assertion en termes "
            "métier, lisible par un responsable non-développeur. Jamais en anglais, même "
            "si les colonnes le sont. Sans noms de colonnes/CTEs ni mots-clés SQL. "
            "✓ Bon : 'Le montant total est toujours positif.' "
            "'Chaque commande appartient à un client actif.' "
            "✗ À proscrire : 'price > 0 pour toutes les lignes de __result__', "
            "'COALESCE(amount, 0) != NULL dans la CTE finale'."
        )
    )
    expected_condition: str = Field(
        description=(
            "Condition booléenne SQL POSITIVE qui doit être VRAIE pour chaque ligne "
            "de `__result__` quand le test réussit — l'affirmation métier attendue, "
            "exprimée directement (jamais sa négation). MockSQL la négocie lui-même "
            "pour produire la requête de validation. "
            "✓ Bon : `date = '2016-01-02'`, `amount > 0`. "
            "⚠️ Testée sur CHAQUE ligne : `z_score = (SELECT MAX(z_score) FROM __result__)` "
            "n'est correcte que si `__result__` a UNE seule ligne. Sur un résultat "
            "MULTI-lignes, viser une ligne précise (le min/max, la 1ʳᵉ) échoue sur toutes "
            "les autres → utilise le champ `scope` pour restreindre l'univers (cf. `scope`). "
            "✗ INTERDIT : tout `!=`, `<>`, `NOT IN`, `NOT (...)` ou `IS NULL` "
            "destiné à 'vérifier ce qui ne doit PAS être là' — exprime la vérité "
            "positive à la place (au lieu de `date != '2016-01-02'`, écris "
            "`date = '2016-01-02'`). "
            "Utilise UNIQUEMENT les colonnes du schéma de `__result__` (casse exacte) "
            "et, si besoin d'une valeur relative, une sous-requête sur `__result__` "
            "uniquement. N'inclus pas `SELECT`/`WHERE` — seulement l'expression booléenne."
        )
    )
    scope: Optional[str] = Field(
        default=None,
        description=(
            "OPTIONNEL. Sélecteur de lignes : `expected_condition` n'est alors testée que "
            "sur les lignes de `__result__` où `scope` est vrai (les autres sont ignorées). "
            "À utiliser pour affirmer un fait sur UNE ligne précise d'un résultat "
            "MULTI-lignes, en restant POSITIF. "
            "Ex. « la ligne de date la plus ancienne est le dataset X » → "
            '`scope: "date = (SELECT MIN(date) FROM __result__)"`, '
            "`expected_condition: \"dataset_id = 'X'\"`. "
            "Laisse `null` si la condition vaut pour TOUTES les lignes. "
            "Un `scope` qui ne sélectionne aucune ligne fait ÉCHOUER l'assertion "
            "(elle ne testerait rien) — choisis un sélecteur qui matche au moins une ligne. "
            "Mêmes colonnes que `__result__` ; pas de `SELECT`/`WHERE`/`FROM` de tête."
        ),
    )


class _AssertionFix(BaseModel):
    test_name: str
    unit_test_description: str
    unit_test_build_reasoning: str
    tags: List[str]
    suggestions: List[str]


class DiagnosticBlock(BaseModel):
    root_cause: str
    sql_pattern: str
    data_issue: str
    fix_summary: str
    fix_recipe: str
    affected_tables: List[str]
    affected_ctes: List[str]


class _AssertionsAndEvaluation(BaseModel):
    reasoning: str  # chain-of-thought: intention du test, cohérence données/résultat, qualité des assertions
    assertions: List[_Assertion] = Field(min_length=1)
    verdict: Literal["Excellent", "Bon", "Insuffisant"]
    reason_type: Optional[
        Literal["bad_data", "bad_assertions", "bad_description", "needs_validation"]
    ] = None
    explanation: str
    assertion_fix: Optional[_AssertionFix] = None
    diagnostic: Optional[DiagnosticBlock] = None
    # Rempli UNIQUEMENT si reason_type == "needs_validation" : nombre de lignes que la
    # description suppose en sortie (cardinalité annoncée), pour construire la question de
    # validation « le résultat produit N lignes alors que tu en attendais M ».
    expected_row_count: Optional[int] = None

    @model_validator(mode="after")
    def _diagnostic_required_for_bad_data(self) -> "_AssertionsAndEvaluation":
        if self.reason_type == "bad_data" and self.diagnostic is None:
            self.diagnostic = DiagnosticBlock(
                root_cause="Données d'entrée insuffisantes ou incohérentes avec la logique SQL",
                sql_pattern="(non déterminé automatiquement)",
                data_issue="Le LLM n'a pas fourni d'analyse détaillée",
                fix_summary="Régénérer les données en ciblant la contrainte SQL du test.",
                fix_recipe="Régénérer les données en ciblant la contrainte SQL identifiée dans le reasoning",
                affected_tables=[],
                affected_ctes=[],
            )
        return self


def _assertion_to_executable(a: _Assertion) -> Dict[str, Any]:
    """Convertit une assertion générée (condition positive) en dict exécutable aval.

    Conserve `description` et `expected_condition` (forme positive, pour l'UI/transparence)
    et dérive `sql` — l'artefact dbt-style réellement exécuté par `_evaluate_assertions`.
    """
    scope = getattr(a, "scope", None)
    return {
        "description": a.description,
        "expected_condition": a.expected_condition,
        **({"scope": scope} if scope and scope.strip() else {}),
        "sql": _assertion_sql_from_condition(a.expected_condition, scope),
    }


def _load_existing_tests(session_id: str) -> List[Dict[str, Any]]:
    """Load the persisted test suite from the test file."""
    test = get_test(session_id)
    if test:
        return test.get("test_cases", [])
    return []


async def run_on_examples(state: "QueryState") -> Dict[str, Any]:
    """
    Exécute les unit tests sur les données générées et renvoie les résultats.
    """
    if state.get("error"):
        return {}

    rerun_all = state.get("rerun_all_tests", False)

    # Contexte commun
    session_id_duckdb = state["session"].replace("-", "_")
    dialect = state["dialect"]
    from models.schemas import get_schemas

    schemas = await get_schemas(project_id=state["project"])
    used_columns = [json.loads(c) for c in state.get("used_columns") or []]

    logger.debug(
        "\n[DEBUG] >>> run_on_examples : used_columns bruts récupérés depuis le state:"
    )
    for uc in used_columns:
        logger.debug(f"      - {uc}")

    filtered_schemas = filter_schemas_by_used_columns(schemas, used_columns)

    # Détermination de la liste de tests à exécuter
    if rerun_all:
        # Charger tous les tests existants depuis la DB
        existing_tests = _load_existing_tests(state["session"])
        # Ajouter/remplacer avec le nouveau test du générateur (s'il y en a un)
        examples_msgs = examples_state_retriever(state)
        if examples_msgs:
            new_test = json.loads(examples_msgs[-1].content)
            if isinstance(new_test, dict):
                merged = {t["test_index"]: t for t in existing_tests}
                merged[new_test["test_index"]] = new_test
                unit_tests = sorted(merged.values(), key=lambda x: int(x["test_index"]))
            else:
                unit_tests = existing_tests
        else:
            unit_tests = existing_tests
    else:
        unit_tests = _parse_unit_tests_from_state(state)
        if unit_tests is None:
            # Le générateur n'a pas produit de nouveau test : ré-exécuter les tests existants
            unit_tests = _load_existing_tests(state["session"])

    if not unit_tests:
        return {}

    # Exécution des tests
    all_tests_results: List[Dict[str, Any]] = []
    with initialize_duckdb(DB_PATH) as con:
        for loop_index, test_case in enumerate(unit_tests):
            logger.debug(
                f"\n[DEBUG] >>> Lancement test {loop_index} avec table(s) : {list(test_case.get('data', {}).keys())}"
            )
            test_result = await _run_single_test_case(
                state=state,
                test_case=test_case,
                loop_index=loop_index,
                session_id=session_id_duckdb,
                query=state.get("optimized_sql"),
                schemas=filtered_schemas,
                used_columns=used_columns,
                con=con,
                dialect=dialect,
                rerun_all=rerun_all,
            )
            all_tests_results.append(test_result)

    global_status = _determine_global_status(all_tests_results)
    content_msg = json.dumps(all_tests_results, indent=2, default=str)
    gen_retries = (
        state.get("gen_retries") if state.get("gen_retries") is not None else 1
    )

    sql = state.get("query", "").strip()
    optimized_sql = state.get("optimized_sql", "").strip()
    examples_msgs = examples_state_retriever(state)
    generated_test_index = (
        examples_msgs[-1].additional_kwargs.get("generated_test_index")
        if examples_msgs
        else None
    )
    results_kwargs = {
        "type": MsgType.RESULTS,
        "parent": (
            state.get("user_message_id") if state.get("input", "").strip() else None
        )
        or state.get("parent_message_id")
        or (state["messages"][-1].id if state.get("messages") else None),
        "request_id": state.get("request_id"),
        **({"sql": sql} if sql else {}),
        **({"optimized_sql": optimized_sql} if optimized_sql else {}),
        **(
            {"generated_test_index": generated_test_index}
            if generated_test_index is not None
            else {}
        ),
        **({"rerun_all": True} if rerun_all else {}),
    }

    return {
        "messages": [
            AIMessage(
                content=content_msg,
                id=str(uuid.uuid4()),
                additional_kwargs=results_kwargs,
            )
        ],
        "status": global_status,
        "gen_retries": gen_retries,
    }


def filter_schemas_by_used_columns(
    schemas: List[dict], used_columns_info: List[dict]
) -> List[dict]:
    """
    Ne garde dans 'schemas' que les tables et colonnes réellement utilisées,
    selon la structure de 'used_columns_info'.

    used_columns_info ressemble à :
    [
      {
        "table": "REF_MODELE_MATERIEL",
        "used_columns": [
          "dt_creation_modele_materiel",
          "id_modele_materiel",
          ...
        ]
      },
      ...
    ]
    """
    # 1. Construire un dictionnaire { "NomTable" -> [colonne1, colonne2, ...] }
    used_cols_dict = {
        f"{item['database']}.{item['table']}"
        if item.get("database")
        else item["table"]: [col.lower() for col in item["used_columns"]]
        for item in used_columns_info
    }

    logger.debug(
        "\n[DEBUG] >>> filter_schemas_by_used_columns : used_cols_dict généré:"
    )
    logger.debug(f"      - {used_cols_dict}")

    filtered_schemas = []
    for table_schema in schemas:
        parts = table_schema["table_name"].split(".")
        qualified = ".".join(parts[-2:]) if len(parts) >= 2 else parts[-1]

        if qualified in used_cols_dict:
            wanted_cols = used_cols_dict[qualified]
            logger.debug(
                f"\n[DEBUG] >>> Filtrage de la table {qualified}. wanted_cols: {wanted_cols}"
            )

            filtered_columns = [
                col
                for col in table_schema["columns"]
                if col["name"].lower() in wanted_cols
                or any(col["name"].lower().startswith(f"{w}.") for w in wanted_cols)
            ]

            logger.debug(
                f"[DEBUG] >>> Table {qualified} - Colonnes conservées: {[c['name'] for c in filtered_columns]}"
            )

            if filtered_columns:
                filtered_schemas.append(
                    {
                        "table_name": table_schema["table_name"],
                        "description": table_schema.get("description", ""),
                        "columns": filtered_columns,
                        "primary_keys": table_schema.get("primary_keys", []),
                    }
                )

    return filtered_schemas


def _parse_unit_tests_from_state(state: QueryState) -> Optional[List[Dict[str, Any]]]:
    """
    Récupère la liste de unit tests depuis l'état.
    Priorité : user_tables > EXAMPLES en mémoire.
    Retourne None si aucun test n'est disponible en mémoire (signal : charger depuis la DB).
    """
    if state["user_tables"] and state["user_tables"] != "":
        unit_tests = json.loads(state["user_tables"])
        if isinstance(unit_tests, dict):
            unit_tests = [unit_tests]
        return unit_tests

    examples_msgs = examples_state_retriever(state)
    if not examples_msgs:
        return None  # Aucun test en mémoire : l'appelant chargera depuis le fichier

    test = json.loads(examples_msgs[-1].content)
    if isinstance(test, dict):
        return [test]
    if isinstance(test, list):
        return test
    return None


def _extract_columns(expr: exp.Expression) -> List[exp.Expression]:
    """
    Trouve toutes les colonnes (exp.Column) dans l'expression fournie
    et les retourne en tant qu'expressions prêtes à être mises dans un SELECT.
    """
    return list(expr.find_all(exp.Column))


def _decompose_cte_in_steps(cte_sql_code: str, dialect: str) -> List[Dict[str, str]]:
    """
    Décompose le code SQL d'une CTE (ou requête) en plusieurs étapes, avec :
      - 1 étape par condition si un JOIN comporte un ON avec plusieurs conditions (via AND).
      - Par défaut, on force désormais chaque JOIN en FULL JOIN sauf si la jointure est latérale (UDTF).
      - Au lieu de COUNT(0), on affiche toutes les colonnes détectées dans la clause ON.
    On retourne une liste de dicts: [{"name": "...", "code": "..."}].
    """
    steps = []
    parsed = sqlglot.parse_one(cte_sql_code, read=dialect)

    # Récupération des parties importantes
    from_expr = parsed.args.get("from_")  # exp.From
    joins_expr = parsed.args.get("joins") or []
    where_expr = parsed.args.get("where")

    def build_query(select_list, from_part, joins_part=None, where_part=None):
        """
        Construit une requête SELECT complète à partir des différents blocs
        (SELECT, FROM, JOIN, WHERE) puis retourne son code SQL en dialecte spécifié.
        """
        query_exp = exp.Select()

        # SELECT
        if select_list:
            query_exp.set("expressions", select_list)
        else:
            # fallback si besoin
            query_exp.set(
                "expressions",
                [exp.Star()],  # ou exp.Count(this=exp.Literal.number(0)) au choix
            )

        # FROM
        if from_part is not None:
            query_exp.set("from", from_part)

        # JOINS
        if joins_part:
            query_exp.set("joins", joins_part)

        # WHERE
        if where_part:
            query_exp.set("where", where_part)

        return query_exp.sql(dialect=dialect)

    # On stocke la table de départ
    tables = []
    if from_expr:
        tables.append(from_expr)

    # -------------------------------------------------------------------------
    # Parcours de chaque JOIN pour générer des étapes
    # -------------------------------------------------------------------------
    join_steps = []
    from sqlglot.expressions import UDTF  # Pour identifier les UDTF (ex: UNNEST)

    for j_idx, join_expr in enumerate(joins_expr, start=1):
        # Copie pour ne pas altérer l'original
        join_copy = join_expr.copy()

        # Si la jointure n'est pas une UDTF (donc pas latérale implicite), forcer le FULL JOIN
        if not isinstance(join_copy.this, UDTF):
            join_copy.set("side", "FULL")
            join_copy.set("kind", None)
        # Sinon, on laisse la jointure en l'état

        # Récupérer la clause ON, s’il y en a une, pour déterminer les colonnes
        on_clause = join_copy.args.get("on")
        if on_clause:
            # Décomposition via AND
            conditions = _extract_conditions(on_clause)
            if len(conditions) > 1:
                # On génère une requête par condition
                for c_idx, cond in enumerate(conditions, start=1):
                    single_join_expr = join_copy.copy()
                    # On remplace la clause ON par une seule condition
                    single_join_expr.set("on", cond)

                    # Récupération de toutes les colonnes présentes dans la condition
                    columns_in_cond = _extract_columns(cond)
                    # fallback si aucune colonne détectée
                    if not columns_in_cond:
                        columns_in_cond = [exp.Star()]

                    step_sql = build_query(
                        select_list=columns_in_cond,
                        from_part=tables[0],
                        joins_part=(tables[1:] if len(tables) > 1 else [])
                        + [single_join_expr],
                    )
                    join_steps.append(
                        {"name": f"step_join_{j_idx}_cond_{c_idx}", "code": step_sql}
                    )
            else:
                # Une seule condition => un seul step
                cond = conditions[0] if conditions else None
                columns_in_cond = _extract_columns(cond) if cond else []
                if not columns_in_cond:
                    columns_in_cond = [exp.Star()]

                step_sql = build_query(
                    select_list=columns_in_cond,
                    from_part=tables[0],
                    joins_part=(tables[1:] if len(tables) > 1 else []) + [join_copy],
                )
                join_steps.append({"name": f"step_join_{j_idx}", "code": step_sql})
        else:
            # JOIN sans clause ON => un step unique
            step_sql = build_query(
                select_list=[exp.Star()],
                from_part=tables[0],
                joins_part=(tables[1:] if len(tables) > 1 else []) + [join_copy],
            )
            join_steps.append({"name": f"step_join_{j_idx}", "code": step_sql})

        # On ajoute ce join à la liste "tables" pour construire la suite
        tables.append(join_expr)

    # On ajoute tous les steps de joins
    steps.extend(join_steps)

    # -------------------------------------------------------------------------
    # Gérer la clause WHERE (exemple : un step "avant WHERE" et un step COUNTIF si on veut)
    # -------------------------------------------------------------------------
    if where_expr:
        # Étape "avant WHERE"
        step_sql_before_where = build_query(
            select_list=[exp.Star()],
            from_part=tables[0],
            joins_part=tables[1:] if len(tables) > 1 else None,
        )
        steps.append({"name": "step_before_where", "code": step_sql_before_where})

        # Étape "COUNTIF par condition de WHERE"
        countif_expressions = _build_countif_expressions(where_expr)
        step_sql_where = build_query(
            select_list=countif_expressions,
            from_part=tables[0],
            joins_part=tables[1:] if len(tables) > 1 else None,
            where_part=None,  # On retire la clause WHERE pour ne faire que le COUNTIF
        )
        steps.append({"name": "step_where", "code": step_sql_where})

    # -------------------------------------------------------------------------
    # Étape finale : la requête complète telle qu’elle était
    # -------------------------------------------------------------------------
    full_sql = parsed.sql(dialect=dialect)
    steps.append({"name": "", "code": full_sql})

    return steps


def _extract_conditions(expr: exp.Expression) -> List[exp.Expression]:
    """
    Extrait récursivement toutes les conditions d'une expression en décomposant
    les noeuds And. Si l'expression n'est pas un And, elle est retournée seule.
    Les doublons (même SQL généré) sont supprimés en conservant l'ordre.
    """

    def _recurse(e: exp.Expression) -> List[exp.Expression]:
        if isinstance(e, exp.And):
            return _recurse(e.this) + _recurse(e.expression)
        return [e]

    seen: dict[str, bool] = {}
    result = []
    for cond in _recurse(expr):
        key = cond.sql()
        if key not in seen:
            seen[key] = True
            result.append(cond)
    return result


def _build_countif_expressions(where_expr: exp.Expression) -> List[exp.Expression]:
    """
    Construit une liste de COUNTIF(...) à partir des conditions extraites de l'expression WHERE.

    Par exemple, pour un WHERE équivalent à "col1 > 10 AND col2 = 'ABC'",
    on génère :
       [COUNTIF(col1 > 10) AS count_cond1, COUNTIF(col2 = 'ABC') AS count_cond2]

    Pour des clauses plus complexes (avec des OR ou des parenthèses imbriquées),
    il faudra éventuellement affiner la logique.
    """
    # Extraction des conditions à partir de l'expression (souvent where_expr correspond à parsed.args.get("where").this)
    conditions = _extract_conditions(where_expr.this)

    countif_list = []
    for idx, cond in enumerate(conditions, start=1):
        # On crée un noeud COUNTIF enveloppé dans un alias
        countif_node = exp.Alias(
            this=exp.CountIf(this=cond), alias=exp.Identifier(this=f"count_cond{idx}")
        )
        countif_list.append(countif_node)

    return countif_list


def _build_cte_sql_with_suffix(
    sql_code: str, last_query_decomposed: List[Dict[str, Any]], suffix: str
) -> str:
    """
    Remplace toutes les occurrences des noms de CTE dans 'sql_code' par un nom suffixé
    afin d'éviter des collisions dans DuckDB.
    (Ici, on ne fait PAS d'exception pour la dernière CTE,
     car on veut vraiment suffixer toute référence aux CTE antérieures.)
    """
    cte_names = [c["name"] for c in last_query_decomposed]
    for dependency in cte_names:
        # Suffixage
        sql_code = sql_code.replace(f"`{dependency}`", f"`{dependency}_{suffix}`")
    return sql_code


def _joined_alias(join_expr: exp.Expression) -> Optional[str]:
    """Lowercased alias (or name) of the table newly introduced by `join_expr`."""
    src = join_expr.this
    if src is None:
        return None
    alias = (getattr(src, "alias", "") or "") or (src.name or "")
    return alias.lower() or None


def _extract_right_key_from_join(join_expr: exp.Expression) -> Optional[exp.Column]:
    """Return the join-key column belonging to the **newly joined** table.

    The ON clause may be written either way (`joined.col = base.col` or
    `base.col = joined.col`), so the syntactic right operand is unreliable: it can
    point at the base table and make the step-trace miss the real non-match. We
    therefore prefer the column qualified by the join's own alias, falling back to
    the previous heuristic (right operand of the first equality).
    """
    on = join_expr.args.get("on")
    if on:
        eqs = list(on.find_all(exp.EQ))
        joined = _joined_alias(join_expr)
        if joined:
            for eq in eqs:
                for col in (eq.this, eq.expression):
                    if (
                        isinstance(col, exp.Column)
                        and (col.table or "").lower() == joined
                    ):
                        return col
        for eq in eqs:
            right = eq.expression
            if isinstance(right, exp.Column):
                return right
        cols = list(on.find_all(exp.Column))
        if cols:
            return cols[-1]
    using = join_expr.args.get("using")
    if using and isinstance(using, list):
        for item in using:
            if isinstance(item, exp.Column):
                return item
            if isinstance(item, exp.Identifier):
                return exp.column(item.name)
    return None


def _build_count_steps_query(
    cte_code: str,
    preceding_ctes: List[Dict[str, str]],
    dialect: str,
) -> tuple[str, List[str]]:
    """Single query with SUM(CASE WHEN …) columns for each JOIN then each WHERE condition.

    All INNER JOINs are converted to LEFT JOINs so every base row is preserved.
    Returns (full_sql, labels) where labels[i] describes the i-th SELECT column.
    """
    tree = sqlglot.parse_one(cte_code, read=dialect)
    from_expr: Optional[exp.Expression] = tree.args.get("from") or tree.args.get(
        "from_"
    )
    joins: List[exp.Expression] = tree.args.get("joins") or []
    where: Optional[exp.Expression] = tree.args.get("where")

    # Un LEFT/RIGHT/FULL JOIN ne filtre pas (la ligne de base survit sans match) —
    # sauf s'il est rendu forçant par un prédicat WHERE non null-tolérant. On réutilise
    # la même classification que la génération focalisée (cte_graph) pour ne PAS
    # étiqueter à tort un LEFT JOIN non-matché comme « étape bloquante » : seuls les
    # INNER JOINs et les OUTER JOINs forçants éliminent réellement des lignes.
    from build_query.cte_graph import _forced_outer_aliases

    forced = _forced_outer_aliases(tree) if isinstance(tree, exp.Select) else set()

    labels: List[str] = []
    select_parts: List[str] = ["COUNT(*) AS base_count"]
    base_name = from_expr.this.alias_or_name if from_expr else "base"
    labels.append(base_name)

    join_null_conditions: List[str] = []
    left_join_sqls: List[str] = []

    for i, join in enumerate(joins):
        side = (join.args.get("side") or "").upper()
        is_outer = side in {"LEFT", "RIGHT", "FULL"}
        joined_alias = _joined_alias(join)
        # Un OUTER JOIN ne filtre que s'il est forçant ; un INNER JOIN filtre toujours.
        filters = (not is_outer) or (joined_alias in forced)

        join_copy = join.copy()
        join_copy.set("side", "LEFT")
        join_copy.set("kind", None)
        left_join_sqls.append(join_copy.sql(dialect=dialect))

        right_col = _extract_right_key_from_join(join)
        if right_col is not None and filters:
            col_sql = right_col.sql(dialect=dialect)
            join_null_conditions.append(f"{col_sql} IS NOT NULL")
            cumul = " AND ".join(join_null_conditions)
            select_parts.append(
                f"SUM(CASE WHEN {cumul} THEN 1 ELSE 0 END) AS after_join_{i + 1}"
            )
            labels.append(f"+ JOIN ({col_sql} IS NOT NULL)")
        else:
            # Join optionnel : on suit le cumul courant (fan-out visible) sans
            # ajouter de condition `IS NOT NULL` — la non-correspondance est voulue.
            if join_null_conditions:
                cumul = " AND ".join(join_null_conditions)
                select_parts.append(
                    f"SUM(CASE WHEN {cumul} THEN 1 ELSE 0 END) AS after_join_{i + 1}"
                )
            else:
                select_parts.append(f"COUNT(*) AS after_join_{i + 1}")
            lbl = joined_alias or (right_col.table if right_col else str(i + 1))
            side_txt = f"{side} " if is_outer else ""
            labels.append(f"+ {side_txt}JOIN {lbl} (préservé)")

    where_conds = _extract_conditions(where.this) if where else []
    cumul_parts = list(join_null_conditions)

    for j, cond in enumerate(where_conds):
        cond_sql = cond.sql(dialect=dialect)
        cumul_parts.append(f"({cond_sql})")
        cumul = " AND ".join(cumul_parts)
        select_parts.append(
            f"SUM(CASE WHEN {cumul} THEN 1 ELSE 0 END) AS after_cond_{j + 1}"
        )
        labels.append(f"+ WHERE {cond_sql}")

    from_sql = from_expr.sql(dialect=dialect) if from_expr else ""
    joins_sql = ("\n" + "\n".join(left_join_sqls)) if left_join_sqls else ""
    select_cols = ",\n  ".join(select_parts)
    body = f"SELECT\n  {select_cols}\n{from_sql}{joins_sql}"

    if preceding_ctes:
        with_parts = [f"`{c['name']}` AS ({c['code']})" for c in preceding_ctes]
        return f"WITH {', '.join(with_parts)}\n{body}", labels

    return body, labels


async def _run_cte_step_trace(
    ctes: list, failing_idx: int, suffix: str, project: str, dialect: str, con
) -> list:
    """Step-level breakdown for a failing CTE (row_count==0).

    Runs a single query with cumulative SUM(CASE WHEN …) columns so the generator knows
    exactly which JOIN condition or WHERE predicate filters out all rows.
    Returns [{label, count}].
    """
    cte = ctes[failing_idx]
    preceding = [c for c in ctes[:failing_idx] if c["name"] != "final_query"]

    try:
        full_sql, labels = _build_count_steps_query(cte["code"], preceding, dialect)
    except Exception:
        return []

    try:
        df, _ = await run_query_on_test_dataset(full_sql, suffix, project, dialect, con)
    except Exception:
        return []

    if df.empty:
        return [{"label": lbl, "count": 0} for lbl in labels]

    row = df.iloc[0].to_dict()
    col_names = list(row.keys())
    return [
        {"label": lbl, "count": int(row.get(col_names[i], 0) or 0)}
        for i, lbl in enumerate(labels)
        if i < len(col_names)
    ]


def _single_alias_of(expr: exp.Expression) -> Optional[str]:
    """Alias (lowercase) qualifiant TOUTES les colonnes de *expr*, ou None."""
    aliases = {(c.table or "").lower() for c in expr.find_all(exp.Column) if c.name}
    aliases.discard("")
    return next(iter(aliases)) if len(aliases) == 1 else None


async def _run_join_predicate_breakdown(
    ctes: list, failing_idx: int, suffix: str, project: str, dialect: str, con
) -> list:
    """Décomposition par prédicat des JOINs filtrants de la CTE bloquante.

    L'étiquette cumulative ``+ JOIN (col IS NOT NULL)`` du step-trace peut désigner
    la mauvaise colonne quand le ON porte plusieurs prédicats (incident 2026-06-11 :
    l'agent a patché `cd_chef_file` alors que le prédicat bloquant était l'égalité
    sur `code_produit_bpce_ps`). Ici chaque égalité du ON est évaluée
    **indépendamment** sur les données réelles : ensembles DISTINCT des deux côtés
    (requêtes DuckDB triviales) + nombre de valeurs communes, prédicat fautif marqué
    ``← BLOQUANT``. Retourne une liste de lignes texte prêtes pour le diagnostic.
    """
    cte = ctes[failing_idx]
    preceding = [c for c in ctes[:failing_idx] if c["name"] != "final_query"]
    try:
        tree = sqlglot.parse_one(cte["code"], read=dialect)
    except Exception:
        return []
    if not isinstance(tree, exp.Select):
        return []
    from_expr = tree.args.get("from") or tree.args.get("from_")
    joins = tree.args.get("joins") or []
    if from_expr is None or not joins:
        return []

    from build_query.cte_graph import _forced_outer_aliases

    try:
        forced = _forced_outer_aliases(tree)
    except Exception:
        forced = set()

    # alias (lowercase) → source SQL rendue avec son alias, prête pour un FROM
    sources: Dict[str, str] = {}

    def _register(src) -> None:
        if src is None:
            return
        alias = (getattr(src, "alias", "") or "") or (getattr(src, "name", "") or "")
        if alias:
            sources[alias.lower()] = src.sql(dialect=dialect)

    _register(from_expr.this)
    for j in joins:
        _register(j.this)

    with_prefix = ""
    if preceding:
        with_parts = [f"`{c['name']}` AS ({c['code']})" for c in preceding]
        with_prefix = "WITH " + ",\n".join(with_parts) + "\n"

    async def _distinct_values(side_expr: exp.Expression, alias: str) -> list:
        sql = (
            f"{with_prefix}SELECT DISTINCT {side_expr.sql(dialect=dialect)} AS v "
            f"FROM {sources[alias]} LIMIT 50"
        )
        df, _ = await run_query_on_test_dataset(sql, suffix, project, dialect, con)
        return ["NULL" if v is None or v != v else str(v) for v in df.iloc[:, 0]]

    def _fmt_set(vals: list) -> str:
        shown = ", ".join(vals[:5])
        more = f", … ({len(vals)} valeurs)" if len(vals) > 5 else ""
        return "{" + shown + more + "}"

    def _unwrap(e: exp.Expression) -> exp.Expression:
        while isinstance(e, exp.Paren):
            e = e.this
        return e

    async def _eq_line(eq: exp.EQ, cte_code: str) -> Optional[tuple]:
        """``(ligne de diagnostic, satisfiable)`` pour une égalité, ou None."""
        lhs, rhs = eq.this, eq.args.get("expression")
        la = _single_alias_of(lhs) if lhs is not None else None
        ra = _single_alias_of(rhs) if rhs is not None else None
        if not (la and ra and la != ra and la in sources and ra in sources):
            return None
        try:
            lvals = await _distinct_values(lhs, la)
            rvals = await _distinct_values(rhs, ra)
        except Exception as exc:
            logger.debug(
                "join breakdown failed for %s: %s — sql: %s",
                eq.sql(dialect=dialect),
                exc,
                cte_code[:500],
            )
            return None
        common = (set(lvals) & set(rvals)) - {"NULL"}
        return (
            f"{eq.sql(dialect=dialect)} → {len(common)} valeur(s) commune(s) — "
            f"gauche {_fmt_set(lvals)}, droite {_fmt_set(rvals)}",
            bool(common),
        )

    async def _is_null_line(is_node: exp.Is) -> Optional[tuple]:
        """``(ligne de diagnostic, satisfiable)`` pour ``<expr> IS NULL``, ou None."""
        if not isinstance(is_node.args.get("expression"), exp.Null):
            return None
        target = is_node.this
        alias = _single_alias_of(target)
        if alias is None or alias not in sources:
            return None
        try:
            vals = await _distinct_values(target, alias)
        except Exception:
            return None
        has_null = "NULL" in vals
        detail = (
            "satisfaite (NULL présent)"
            if has_null
            else f"aucune valeur NULL — valeurs {_fmt_set(vals)}"
        )
        return f"{is_node.sql(dialect=dialect)} → {detail}", has_null

    lines: list = []
    for join in joins:
        side = (join.args.get("side") or "").upper()
        is_outer = side in {"LEFT", "RIGHT", "FULL"}
        joined_alias = _joined_alias(join)
        if is_outer and joined_alias not in forced:
            continue  # join non filtrant : la non-correspondance est tolérée
        on = join.args.get("on")
        if on is None:
            continue

        pred_lines: list = []
        for pred in _extract_conditions(on):
            pred_sql = pred.sql(dialect=dialect)
            inner = _unwrap(pred)
            decomposed = False
            if isinstance(inner, exp.EQ):
                res = await _eq_line(inner, cte["code"])
                if res is not None:
                    line, satisfiable = res
                    marker = "" if satisfiable else " ← BLOQUANT"
                    pred_lines.append(line + marker)
                    decomposed = True
            elif isinstance(inner, exp.Or):
                # Un OR (typiquement `clé = … OR clé IS NULL`) ne bloque que si
                # AUCUNE branche n'est satisfiable — l'évaluer branche par
                # branche, sinon c'est précisément le prédicat fautif qui reste
                # affiché « non décomposé » (incident 2026-06-11).
                branch_lines: list = []
                satisfiable_flags: list = []
                for branch in inner.flatten():
                    branch = _unwrap(branch)
                    if isinstance(branch, exp.EQ):
                        res = await _eq_line(branch, cte["code"])
                    elif isinstance(branch, exp.Is):
                        res = await _is_null_line(branch)
                    else:
                        res = None
                    if res is None:
                        branch_lines.append(
                            f"{branch.sql(dialect=dialect)} → (branche non décomposée)"
                        )
                        satisfiable_flags.append(None)
                    else:
                        branch_lines.append(res[0])
                        satisfiable_flags.append(res[1])
                if any(f is not None for f in satisfiable_flags):
                    blocking = all(f is False for f in satisfiable_flags)
                    marker = (
                        " ← BLOQUANT (aucune branche du OR n'est satisfiable)"
                        if blocking
                        else ""
                    )
                    pred_lines.append(f"{pred_sql} — par branche :{marker}")
                    pred_lines.extend(f"  · {bl}" for bl in branch_lines)
                    decomposed = True
            if not decomposed:
                pred_lines.append(f"{pred_sql} → (prédicat non décomposé)")

        if pred_lines:
            lines.append(f"JOIN {joined_alias or '?'} — décomposition par prédicat :")
            lines.extend(f"  {pl}" for pl in pred_lines)
    return lines


async def _run_cte_trace(
    ctes: list, suffix: str, project: str, dialect: str, con
) -> dict:
    """
    For each CTE, builds a WITH ... SELECT * FROM cteN query and runs it to capture row counts.
    For CTEs that return 0 rows, also runs a step-by-step breakdown (per JOIN/WHERE condition).
    Returns {"cte_name": {"row_count": N, "steps": [...]}} for every non-final CTE.
    """
    trace = {}
    for i, cte in enumerate(ctes):
        if cte["name"] == "final_query":
            continue
        with_parts = [
            f"`{ctes[j]['name']}` AS ({ctes[j]['code']})" for j in range(i + 1)
        ]
        sql = "WITH " + ",\n".join(with_parts) + f"\nSELECT * FROM `{cte['name']}`"
        try:
            df, _ = await run_query_on_test_dataset(sql, suffix, project, dialect, con)
            row_count = df.shape[0]
            result: dict = {"row_count": row_count}
            if row_count == 0:
                steps = await _run_cte_step_trace(
                    ctes, i, suffix, project, dialect, con
                )
                if steps:
                    result["steps"] = steps
            trace[cte["name"]] = result
        except Exception as e:
            # Message DuckDB + SQL de l'étape : sans eux, impossible de distinguer
            # un vrai problème (types, colonne absente) d'une simple conséquence du
            # 0-ligne amont (règle projet : toujours logger la requête fautive).
            logger.warning(
                "[executor] CTE trace `%s` : %s — sql:\n%s", cte["name"], e, sql
            )
            trace[cte["name"]] = {
                "row_count": -1,
                "error": str(e),
                "sql": cte["code"],
            }
    return trace


def _select_failing_cte(ctes: list, cte_trace: dict, dialect: str) -> Optional[str]:
    """Pick the CTE to target for correction and annotate `cte_trace` in place.

    Naively taking the first empty CTE mislabels LEFT-optional / anti-join CTEs as
    blockers (cf. TMP_MR / SIRET_ONUS dans c1). We defer to
    `cte_graph.classify_blocking_ctes`, which keeps only CTEs reachable from the
    final result via *required* edges (FROM / INNER / forcing OUTER). Each empty CTE
    gets a `blocking` flag so the diagnostic hint can stop alarming on optional ones.

    Falls back to the first empty CTE if classification is unavailable.
    """
    blocking_order: list = []
    try:
        from build_query.cte_graph import classify_blocking_ctes

        blocking_order = classify_blocking_ctes(ctes, cte_trace, dialect)
    except Exception:
        blocking_order = []

    if blocking_order:
        blocking = set(blocking_order)
        for name, info in cte_trace.items():
            if isinstance(info, dict) and info.get("row_count") == 0:
                info["blocking"] = name in blocking
        return blocking_order[0]

    return next(
        (name for name, info in cte_trace.items() if info.get("row_count") == 0),
        None,
    )


async def _run_single_test_case(
    state: QueryState,
    test_case: Dict[str, Any],
    loop_index: int,
    session_id: str,
    query: str,
    schemas: list,
    used_columns: Optional[List[Dict[str, List[str]]]],
    con,
    dialect,
    rerun_all: bool = False,
) -> Dict[str, Any]:
    """
    Exécute la logique d'un seul cas de test.
    Retourne un dict fusionné contenant les métadonnées du test (issues du LLM)
    et les résultats d'exécution DuckDB. Les erreurs sont capturées dans le résultat.
    test_index provient du test_case lui-même pour conserver l'identifiant logique.
    """
    # Preserve the logical test_index from the test case (string like "1", "2"…)
    test_index = test_case.get("test_index", str(loop_index))
    base = {
        "test_index": test_index,
        "test_name": test_case.get("test_name", ""),
        "unit_test_description": test_case.get("unit_test_description", ""),
        "unit_test_build_reasoning": test_case.get("unit_test_build_reasoning", ""),
        "tags": test_case.get("tags", []),
        "suggestions": test_case.get("suggestions", []),
        "data": test_case.get("data", {}),
    }

    try:
        # 1) Préparation et insertion des données de test
        test_data = _prepare_test_data(test_case, schemas)
        suffix = f"{session_id}{test_index}"

        logger.debug("Creating temp tables for suffix=%s", suffix)

        logger.diag(
            "[executor] tables dans les données: %s",
            list(test_case.get("data", {}).keys()),
        )
        for tname, rows in test_case.get("data", {}).items():
            logger.diag(
                "  %s: %s ligne(s)", tname, len(rows) if isinstance(rows, list) else "?"
            )

        # Création des tables de test dans DuckDB + insertion
        # Toujours overwrite=True : chaque passage (retry inclus) repart sur des tables fraîches.
        # L'ancien overwrite=False sur empty_results accumulait les anciennes lignes + les nouvelles,
        # causant des conflits dans les CTEs qui lisent les mêmes tables (ex: SIRET_ONUS).
        logger.diag(
            "[executor] overwrite=True (status précédent=%s)", state.get("status")
        )
        from utils.timing import atimed

        async with atimed("exec:duckdb_setup+query"):
            duckdb_tables_schema = create_test_tables(
                tables=schemas,
                suffix=suffix,
                overwrite=True,
                con=con,
                dialect=dialect,
            )
            insert_queries = insert_examples(
                data_dict=test_data,
                schemas=duckdb_tables_schema,
                suffix=suffix,
                used_columns=used_columns,
            )
            execute_queries(list(insert_queries), con)
            # 2) On exécute la requête globale
            final_res_df, final_duckdb_sql = await run_query_on_test_dataset(
                query, suffix, state["project"], dialect, con
            )
        logger.diag("[executor] DuckDB SQL exécuté:\n%s", final_duckdb_sql[:2000])
        logger.diag("[executor] résultat: %s ligne(s)", len(final_res_df))

        if final_res_df.empty:
            ctes = json.loads(state.get("query_decomposed") or "[]")
            cte_trace = await _run_cte_trace(
                ctes, suffix, state["project"], dialect, con
            )
            failing_cte = _select_failing_cte(ctes, cte_trace, dialect)
            # Décomposition par prédicat des JOINs de la CTE bloquante : nomme le
            # prédicat fautif avec les valeurs des deux côtés (quelques requêtes
            # DuckDB triviales), là où l'étiquette cumulative peut désigner la
            # mauvaise colonne.
            if failing_cte and cte_trace.get(failing_cte, {}).get("row_count") == 0:
                failing_idx = next(
                    (i for i, c in enumerate(ctes) if c["name"] == failing_cte), None
                )
                if failing_idx is not None:
                    try:
                        breakdown = await _run_join_predicate_breakdown(
                            ctes, failing_idx, suffix, state["project"], dialect, con
                        )
                        if breakdown:
                            cte_trace[failing_cte]["join_breakdown"] = breakdown
                    except Exception as exc:
                        logger.debug(
                            "join predicate breakdown failed for %s: %s",
                            failing_cte,
                            exc,
                        )
            return {
                **base,
                "status": "empty_results",
                "results_json": await format_result(final_res_df),
                "cte_trace": cte_trace,
                "failing_cte": failing_cte,
                "assertion_results": [],
            }

        existing_assertions = [
            a for a in (test_case.get("assertion_results") or []) if a.get("sql")
        ]

        if rerun_all and existing_assertions:
            # Re-run existing assertions without LLM (user-triggered rerun or SQL update)
            view_name = f"__result__{suffix}"
            con.register(view_name, final_res_df)
            try:
                retry_kwargs = dict(
                    view_name=view_name,
                    con=con,
                    duckdb_sql=final_duckdb_sql,
                    test_data=test_data,
                    result_df=final_res_df,
                    test_description=test_case.get("unit_test_description", ""),
                )
                assertion_results = await _evaluate_assertions_with_retry(
                    existing_assertions, **retry_kwargs
                )
            finally:
                con.execute(f'DROP VIEW IF EXISTS "{view_name}"')
            has_failing = any(not a.get("passed") for a in assertion_results)
            return {
                **base,
                "status": "complete",
                "results_json": await format_result(final_res_df),
                "assertion_results": assertion_results,
                "verdict": "Insuffisant" if has_failing else "Bon",
                "reason_type": "bad_assertions" if has_failing else None,
                "evaluation_explanation": (
                    "Les assertions échouent sur les données re-exécutées."
                    if has_failing
                    else "Les assertions passent sur les données re-exécutées."
                ),
            }

        # Assertions and LLM evaluation are handled by the assertion_generator node
        return {
            **base,
            "status": "complete",
            "results_json": await format_result(final_res_df),
            "assertion_results": [],
        }

    except asyncio.CancelledError:
        logger.warning(
            "[executor] test annulé (CancelledError) — statut error pour history_saver"
        )
        return {
            **base,
            "status": "error",
            "error": "cancelled",
            "results_json": "[]",
        }
    except Exception as e:
        if _is_duckdb_data_error(e):
            logger.warning(
                "[executor] Erreur de données DuckDB → bad_data_error: %s", e
            )
            return {
                **base,
                "status": "bad_data_error",
                "exec_error": str(e),
                "results_json": "[]",
                "assertion_results": [],
            }
        return {
            **base,
            "status": "error",
            "error": str(e),
            "results_json": "[]",
        }


_DUCKDB_DATA_ERROR_PREFIXES = ("Invalid Input Error", "Conversion Error")


def _is_duckdb_data_error(exc: Exception) -> bool:
    msg = str(exc)
    return any(msg.startswith(p) for p in _DUCKDB_DATA_ERROR_PREFIXES)


def _prepare_test_data(
    test_case: Dict[str, Any], schemas: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Récupère les données de test, les parse en JSON, et remplace les valeurs manquantes par NULL.
    """
    test_data_json = test_case.get("data", {})
    return replace_missing_with_null(test_data_json, schemas)


async def _save_step_partial_results(
    cte: Dict[str, Any],
    partial_res: DataFrame,
) -> List[Dict[str, Any]]:
    """
    Construit la liste des résultats partiels :
      - version standard
      - version no_where seulement si has_where == True
    """
    results = [
        {
            "cte_name": cte["name"],
            "sql_code": cte["code"],
            "row_count": partial_res.shape[0],
            "result_json": await format_result(partial_res),
        }
    ]

    return results


async def _handle_test_result(
    state: QueryState,
    test_case: Dict[str, Any],
    test_index: int,
    test_data: Dict[str, Any],
    test_res_df: DataFrame,
    simplified_partial_results: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Gère la construction du résultat final pour un test donné.
    - En cas de DataFrame vide => statut 'empty_results'
    - Sinon => statut 'complete'
    """
    format_res = await format_result(test_res_df)
    if test_res_df.size == 0 and state["gen_retries"] > 0:
        return {
            "test_index": test_index,
            "unit_test_description": test_case.get("unit_test_description", ""),
            "status": "empty_results",
            "test_data": test_data,
            "results_json": format_res,
            "step_by_step_results": simplified_partial_results,
        }

    return {
        "test_index": test_index,
        "unit_test_description": test_case.get("unit_test_description", ""),
        "status": "complete",
        "test_data": test_data,
        "results_json": format_res,
        "step_by_step_results": simplified_partial_results,
    }


REGEN_ASSERTION_LIMIT = 3


async def _generate_assertions_and_evaluate(
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
    extra_instructions: list[str] | None = None,
) -> _AssertionsAndEvaluation:
    """
    Single LLM call that generates 1-N dbt-style assertions AND evaluates test quality.
    Returns an _AssertionsAndEvaluation with assertions, verdict, explanation, and optional fix.
    Falls back to an empty assertions + Bon verdict on failure.
    """
    schema_lines = [f"  - `{col}`: {dtype}" for col, dtype in result_df.dtypes.items()]
    schema_str = "\n".join(schema_lines) if schema_lines else "  (aucune colonne)"
    sample = result_df.head(5).to_dict(orient="records")
    row_count = len(result_df)

    # ── System : rôle + index des sections + règles (préfixe stable → cacheable) ──
    system_content = """Tu es un expert en tests SQL dbt-style avec DuckDB. À partir d'un \
résultat de requête déjà exécuté, tu génères des assertions de validation ET tu évalues la \
qualité du test en un seul appel.

Le message suivant contient ces sections, délimitées par des balises :
- `<test_context>` : la description métier du scénario testé.
- `<result_schema>` : le schéma exact de la table `__result__` (colonnes + types).
- `<query>` : la requête SQL testée.
- `<input_data>` : les données d'entrée injectées dans DuckDB.
- `<result_sample>` : le résultat après exécution (nombre de lignes + exemples).
- `<user_instructions>` (optionnel) : consignes ajoutées par l'utilisateur pendant la génération.
  Prends-les en compte dans ton verdict et tes assertions sans jamais enfreindre les règles
  ci-dessous (notamment l'interdiction des assertions triviales et négatives).
- `<task>` : ce que tu dois produire.

**Méthode — commence par raisonner à voix haute (`reasoning`, 3–5 phrases) :**
- Quelle est l'intention de ce test ? Quel comportement SQL veut-il vérifier ?
- Les données d'entrée sont-elles cohérentes avec cette intention (types, cardinalité, cas limites) ?
- Si la requête contient GROUP BY + agrégat (COUNT, STDDEV, AVG, SUM, MAX…) : est-ce que
  TOUS les groupes ont exactement la même cardinalité ? Ex : 1 ligne par groupe partout →
  COUNT=1 constant → STDDEV=0 → bad_data. En revanche, si les groupes ont des cardinalités
  différentes (ex : 3, 2, 1, 1, 1) → STDDEV calculable → test valide, ne pas signaler bad_data.
  La correction est de dupliquer des lignes sur la même clé GROUP BY, pas d'ajouter de nouvelles valeurs.
- Le résultat DuckDB est-il conforme à ce qu'on attendrait ?
- Les assertions à générer valident-elles réellement la logique métier ? Regarde les exemples de
  `<result_sample>` pour juger : si la colonne vérifiée par `IS NULL` contient déjà des valeurs
  non-nulles dans les exemples, l'assertion est triviale (retourne toujours 0 ligne). Plus
  généralement, une assertion est triviale si elle passe quel que soit le contenu réel du résultat —
  elle ne discrimine pas une bonne réponse d'une mauvaise. Exemples typiques : `WHERE col IS NULL`
  (colonne jamais nulle), `WHERE 1=0`, ou un `NOT IN (...)` dont la liste englobe tous les cas
  possibles sauf un. Si toutes les assertions sont triviales et ne vérifient aucune valeur concrète
  ou invariant réel du résultat, c'est `bad_assertions`.
  Une bonne assertion pince soit la valeur exacte retournée (condition `date = '2026-03-07'`), soit
  un invariant structurel observable (condition `z_score = (SELECT MAX(z_score) FROM __result__)`).
- Si la requête utilise ORDER BY + LIMIT ou OFFSET : est-ce que plusieurs lignes ont exactement la même
  valeur de tri à la position retournée ? Si oui, le résultat est non-déterministe (ex : 3 groupes avec
  le même COUNT → même Z-score → OFFSET 1 retourne n'importe lequel) → `bad_data`. La correction est
  d'assigner des cardinalités distinctes à chaque groupe de façon à avoir un ordre unique.

**Règle du champ `description` (ce que l'utilisateur lit dans l'UI) :**
Chaque assertion porte une `description` : une phrase **en français**, **courte (max 12 mots)**,
en **langage métier** lisible par un responsable non-développeur. Elle affirme ce qui est vérifié,
pas la mécanique SQL.
- OBLIGATOIRE en français — jamais en anglais, même si la requête ou les colonnes sont en anglais.
- INTERDIT : noms de colonnes/CTEs, mots-clés SQL, opérateurs (`>`, `=`, `IS NULL`…), `__result__`.
- ✓ Bon : « Le montant total reste positif. » / « Chaque commande a un client actif. »
- ✗ À proscrire : « price > 0 for every row of __result__ », « COALESCE(amount, 0) != NULL ».

**OBJECTIF — capter les régressions, pas énoncer des évidences :**
Une assertion ne sert à RIEN si elle resterait vraie quand la logique SQL régresse. Le but est
qu'une modification fautive du SQL (mauvais calcul, mauvaise jointure, mauvais filtre) fasse
ÉCHOUER l'assertion. Pour cela, **pince la VALEUR DE SORTIE CONCRÈTE** attendue pour ce scénario,
calculée à partir des données d'entrée injectées (lis `<result_sample>`).
- ✗ FAIBLE — invariants génériques qui survivraient à une régression : `montant > 0`, `montant
  >= 0` ("pas négatif"), `total IS NOT NULL`. Le total peut devenir faux tout en restant positif :
  l'assertion ne capte rien.
- ✓ FORT — la valeur exacte que le scénario doit produire : si les entrées impliquent un total de
  150, écris `total = 150` (pas `total > 0`). Si la date attendue est `2026-01-02`, écris
  `date = '2026-01-02'`. Si le scénario teste un rang/ordre, pince la valeur classée attendue.
- Exception légitime : un invariant non-trivial EST l'objet même du test (ex. le scénario vérifie
  explicitement qu'un solde ne peut jamais être négatif après remboursement) — alors `solde >= 0`
  est valide. Mais par défaut, préfère toujours la valeur exacte.

**Règles des assertions (`expected_condition`) — entre 1 et plusieurs, selon le scénario :**
Chaque assertion fournit une `expected_condition` : une **condition booléenne POSITIVE** qui doit
être VRAIE pour chaque ligne de `__result__` quand le test réussit. **Tu n'écris PAS de SQL
`SELECT`/`WHERE` et tu n'écris JAMAIS la négation** — MockSQL négocie lui-même ta condition pour
produire la requête de validation (0 ligne = OK). Tu exprimes seulement la vérité métier attendue,
à l'affirmative.
- Exprime l'AFFIRMATION, jamais sa négation : pour pincer la valeur retournée `2026-01-02`,
  écris `expected_condition: "date = '2026-01-02'"` — surtout PAS `date != '2026-01-02'`.
  "Vérifier ce qui ne doit pas être là" est INTERDIT : reformule toujours en ce qui DOIT être là.
- UNE dimension par assertion : chaque `expected_condition` ne valide qu'UNE seule propriété
  observable (une colonne, ou un invariant portant sur une colonne). Si le scénario exige de
  vérifier plusieurs propriétés, émets PLUSIEURS assertions — le schéma en accepte 1 à N.
  ✗ INTERDIT — combiner deux colonnes sans rapport avec `OR`/`AND` : un `OR` est vrai dès qu'une
    branche l'est, donc ne pince RIEN et masque la vraie intention ; un `AND` entre colonnes
    d'intentions distinctes fait échouer l'assertion pour une raison ambiguë (on ne sait pas
    quelle propriété est violée).
  ✓ Découpe : une assertion par colonne/propriété, chacune vérifiable seule.
  Exception : `AND` reste permis quand les deux termes décrivent le MÊME invariant sur la MÊME
  colonne (ex. bornes d'un intervalle : `col >= 0 AND col <= 100`).
- Utilise UNIQUEMENT les colonnes de `<result_schema>` (noms exacts, sensibles à la casse).
- N'écris que l'expression booléenne (pas de `SELECT`, pas de `WHERE`, pas de `FROM`).
  Pour une valeur relative, une sous-requête sur `__result__` uniquement est permise :
  `expected_condition: "val = (SELECT MAX(val) FROM __result__)"`.
- ⚠️ Chaque `expected_condition` est testée sur CHAQUE ligne de `__result__`. Donc
  `val = (SELECT MAX(val) FROM __result__)` n'est correcte que si le résultat a UNE seule
  ligne. Sur un résultat MULTI-lignes, viser une ligne précise (min/max, première, une
  clé donnée) échoue à tort sur toutes les autres lignes. Dans ce cas, renseigne `scope` :
  - `scope` (optionnel) sélectionne les lignes sur lesquelles la condition s'applique ;
    les autres sont ignorées. La requête devient `WHERE (scope) AND ((condition) IS NOT TRUE)`.
  - Ex. « la ligne la plus ancienne est le dataset DS_001 » sur un résultat trié multi-lignes :
    `scope: "billing_started_at = (SELECT MIN(billing_started_at) FROM __result__)"`,
    `expected_condition: "dataset_id = 'DS_001'"`. Reste POSITIF, pince une régression de tri.
  - Un `scope` qui ne matche aucune ligne fait ÉCHOUER l'assertion (elle ne testerait rien).
- INTERDIT absolu : ne référence AUCUNE table en dehors de `__result__`.
- INTERDIT — conditions triviales (toujours vraies quelle que soit la valeur du résultat) :
  ✗ une condition que toute ligne satisfait forcément (ex. `1 = 1`, `col = col`)
  ✗ une condition basée sur `IS NOT NULL` d'une colonne déjà non-nulle dans les exemples
  Une condition triviale ne discrimine pas une bonne réponse d'une mauvaise — elle est inutile.
- OBLIGATOIRE — au moins une assertion qui pince la VALEUR CONCRÈTE de sortie (cf. OBJECTIF
  ci-dessus), tirée de `<result_sample>`. C'est elle qui capte les régressions. À fortiori pour
  les requêtes avec agrégat (SUM/COUNT/AVG/MAX), CASE, ou ORDER BY + LIMIT/OFFSET : la valeur
  calculée doit être figée, pas seulement bornée.
  Exemple : si `__result__` contient `{"date": "2026-01-02"}`, écris `expected_condition: "date = '2026-01-02'"`.
  Pour les colonnes date/timestamp, utilise le format `'YYYY-MM-DD'` (ex: `'2016-01-02'`) sans la partie heure.

**Verdict de qualité :**
- `verdict` : "Excellent", "Bon", ou "Insuffisant"
- `reason_type` (uniquement si Insuffisant) : "bad_data" (données d'entrée incorrectes —
  mauvais types, contraintes non respectées, résultat inattendu), "bad_assertions"
  (les assertions générées ne permettent pas de valider ce scénario — y compris si elles
  sont triviales : toujours vraies indépendamment de la valeur réelle du résultat),
  "bad_description" (cf. ci-dessous — la description annonce une sortie que la requête ne
  produit pas), ou "needs_validation" (cf. ci-dessous — la description suppose un NOMBRE de
  lignes différent du réel, mais les données d'entrée sont valides → on demande à l'humain)
- `explanation` : une phrase ultra-concise (max 20 mots) en français, lisible par un responsable métier —
  sans noms de colonnes, de CTEs ni de mots-clés SQL.
  ✓ 'Les données couvrent correctement le scénario nominal.'
  ✓ 'Les valeurs d'entrée ne produisent pas le résultat attendu pour ce cas limite.'
  ✗ 'La CTE orders_filtered retourne 0 lignes car user_id IS NULL.'
- `assertion_fix` (uniquement si `reason_type == "bad_assertions"`) : objet décrivant
  la correction à apporter au test pour permettre une meilleure génération d'assertions :
  - `test_name` : nom court corrigé (3–6 mots)
  - `unit_test_description` : description précise et correcte, sans ambiguïté
  - `unit_test_build_reasoning` : explication de la correction
  - `tags` : liste parmi Logique métier, Null checks, Cas limites, Intégration,
    Valeurs dupliquées, Performance
  - `suggestions` : 2–3 vérifications correctives précises ("Vérifie que …")
  Si `reason_type != "bad_assertions"`, `assertion_fix` doit être `null`.
- `diagnostic` : OBLIGATOIRE si `reason_type == "bad_data"`, sinon `null`.
  Quand `reason_type == "bad_data"`, tu DOIS remplir ce bloc — ne laisse pas null :
  - `root_cause` : phrase courte identifiant la cause (ex: "STDDEV=0 — chaque date n'apparaît qu'une fois")
  - `sql_pattern` : clause SQL en cause (ex: "COUNT(descript) GROUP BY date → variance nulle")
  - `data_issue` : ce qui manque dans les données générées (ex: "6 dates distinctes avec 1 ligne chacune, COUNT=1 partout")
  - `fix_summary` : phrase courte (max 15 mots) lisible par l'utilisateur dans l'UI —
    décrit le mécanisme sans les valeurs concrètes ni les détails techniques.
    ✓ Bon : "Dupliquer des lignes sur la même date pour varier le COUNT par groupe."
    ✓ Bon : "Ajouter une ligne de JOIN manquante pour que la jointure produise un résultat."
    ✗ Interdit : noms de colonnes, de CTEs, valeurs spécifiques, termes SQL.
  - `fix_recipe` : instruction opérationnelle complète passée au correcteur — 4 éléments requis :
    (1) table exacte et champ(s) à modifier (nom exact tel qu'affiché dans `<input_data>`),
    (2) mécanisme précis : pour les bugs GROUP BY/agrégat, écrire impérativement
        "dupliquer N lignes avec [col_group_by]='[valeur]'" — JAMAIS "ajouter des valeurs variables"
        ni aucune formulation abstraite,
    (3) valeurs concrètes tirées des données d'entrée, avec le compte par groupe
        (ex: "'2016-01-02' × 3 lignes, '2016-01-03' × 2 lignes"),
    (4) effet attendu sur le calcul SQL (ex: "→ COUNT ∈ {2, 3} → STDDEV > 0").
    ✗ Interdit : "ajouter des données variables", "modifier les valeurs", tout terme générique.
    ✓ Bon : "Dans [table], dupliquer la ligne [col]='2016-01-02' pour en avoir 3 copies
             et [col]='2016-01-03' pour en avoir 2 → COUNT varie → STDDEV > 0."
  - `affected_tables` : liste des noms de tables dont les données doivent être corrigées
  - `affected_ctes` : liste des CTEs impactées par le problème

**Cohérence description ↔ sortie réelle (`bad_description`) :**
Compare la valeur de sortie ANNONCÉE dans `<test_context>` au résultat réel de `<result_sample>`.
Si la description affirme une valeur de sortie CONCRÈTE (« le total est de 2.0M », « la corrélation
vaut 0.2 », « le résultat attendu est X ») que le résultat réel CONTREDIT (valeur différente), le
test ment au lecteur **même si les assertions passent** (elles ont pu être alignées sur le réel).
Dans ce cas : `verdict: "Insuffisant"` + `reason_type: "bad_description"`, et `explanation` qui pointe
l'écart en langage métier (ex. « La description annonce une valeur que le calcul ne produit pas »).
Les données sont valides — NE les corrige PAS, NE relance rien : c'est le narratif qui est faux.
⚠️ N'utilise ce motif QUE si la description énonce une valeur concrète contredite — JAMAIS pour une
description qualitative ou structurelle (« vérifie que les régions sans trajet n'apparaissent pas »),
ni quand la description ne donne aucune valeur chiffrée précise.

**Cardinalité annoncée ↔ réelle (`needs_validation`) :**
Compare le NOMBRE de lignes que la description suppose en sortie au `row_count` réel de
`<result_sample>`. Si la description suppose une cardinalité précise (« une seule ligne »,
« exactement N lignes », « pour un client avec 2 cartes je m'attends à 1 ligne ») et que le
résultat réel en produit un nombre DIFFÉRENT, **alors qu'aucune donnée n'est cassée** (lignes
genuines, pas de type invalide, sortie non vide), NE tranche PAS toi-même : c'est peut-être la
description qui est trop stricte, peut-être le SQL qui a dérivé. C'est une AMBIGUÏTÉ à déléguer
à l'humain, pas une donnée à corriger.
Dans ce cas : `verdict: "Insuffisant"` + `reason_type: "needs_validation"`, `expected_row_count`
= le nombre de lignes supposé par la description (entier), et `explanation` qui pointe l'écart en
langage métier (ex. « Le scénario suppose 1 ligne mais le calcul en produit 2 — à confirmer »).
NE génère PAS de `diagnostic`, NE corrige PAS les données.
⚠️ N'utilise ce motif QUE pour un écart de CARDINALITÉ avec données valides. Si la sortie est
vide (0 ligne), reste sur le cas « résultat vide » ci-dessous. Si les données sont réellement
incohérentes (mauvais types, contrainte de jointure non satisfaite), utilise `bad_data`. Si la
description ne suppose aucun nombre de lignes précis, n'utilise PAS ce motif.

**Cas particulier — résultat vide intentionnel :** si `<test_context>` mentionne explicitement
"plage vide", "aucune ligne", "filtre qui exclut tout", alors le résultat vide est correct.
Évalue si les données d'entrée sont bien construites pour produire ce vide (Bon/Excellent),
ou si les données ne semblent pas configurées pour ce scénario (Insuffisant + bad_data)."""

    # ── Human : sections balisées dans l'ordre contexte → tables → SQL → input → output → ask ──
    user_instructions_block = ""
    if extra_instructions:
        numbered = "\n".join(f"{i + 1}. {t}" for i, t in enumerate(extra_instructions))
        user_instructions_block = f"""

<user_instructions>
{numbered}
</user_instructions>"""

    human_content = f"""<test_context>
{test_description}
</test_context>

<result_schema>
{schema_str}
</result_schema>

<query>
```sql
{duckdb_sql}
```
</query>

<input_data>
{test_data}
</input_data>

<result_sample>
{row_count} ligne(s) :
{json.dumps(sample, ensure_ascii=False, default=str)}
</result_sample>{user_instructions_block}

<task>
Produis, conformément aux règles du message système :
1. Entre 1 et plusieurs `assertions` (chacune une `expected_condition` positive sur `__result__`).
2. Le `verdict` de qualité (+ `reason_type`, `explanation`, et `assertion_fix`/`diagnostic` selon le cas).
Réponds uniquement avec l'objet structuré demandé.
</task>"""

    llm = make_llm()
    structured_llm = llm.with_structured_output(_AssertionsAndEvaluation)
    try:
        logger.diag("[assertions_eval] human (extrait):\n%s", human_content[:3000])
        result: _AssertionsAndEvaluation = await structured_llm.ainvoke(
            [SystemMessage(content=system_content), HumanMessage(content=human_content)]
        )
        logger.diag("[assertions_eval] reasoning:\n%s", result.reasoning)
        logger.diag(
            "[assertions_eval] verdict=%s reason_type=%s assertions=%s",
            result.verdict,
            result.reason_type,
            len(result.assertions),
        )
        for i, a in enumerate(result.assertions):
            logger.diag(
                "[assertions_eval] [%d] %s | condition: %s",
                i,
                a.description,
                a.expected_condition,
            )
        return result
    except Exception as e:
        logger.diag("[assertions_eval] ERREUR: %s", e)
        return _AssertionsAndEvaluation(
            assertions=[],
            verdict="Bon",
            explanation="Évaluation indisponible.",
        )


async def _generate_diagnostic(
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
    eval_reasoning: str,
) -> Optional[DiagnosticBlock]:
    """Second focused LLM call to produce a surgical DiagnosticBlock when bad_data is detected.
    Uses DiagnosticBlock directly as structured output schema — all fields required, no Optional."""
    sample = result_df.head(5).to_dict(orient="records")
    row_count = len(result_df)

    prompt = f"""Tu es un expert en tests SQL. Le test suivant a été jugé "bad_data" : les données d'entrée ne permettent pas de valider le scénario.

Description du test : {test_description}

Données d'entrée injectées dans DuckDB :
{test_data}

Requête SQL testée :
```sql
{duckdb_sql}
```

Résultat DuckDB — {row_count} ligne(s) :
{sample}

Raisonnement de l'évaluateur :
{eval_reasoning}

Produis une analyse chirurgicale en remplissant TOUS les champs :
- `root_cause` : phrase courte identifiant la cause racine (ex: "STDDEV=0 — chaque date n'apparaît qu'une fois")
- `sql_pattern` : clause SQL en cause (ex: "COUNT(descript) GROUP BY date → variance nulle → STDDEV=0")
- `data_issue` : description précise du défaut dans les données (ex: "6 dates distinctes avec 1 ligne chacune → COUNT=1 partout")
- `fix_summary` : phrase courte (max 15 mots) lisible par l'utilisateur — mécanisme sans détails techniques
  ✓ "Dupliquer des lignes sur la même date pour varier le COUNT par groupe."
  ✗ Noms de colonnes, CTEs, valeurs spécifiques, termes SQL
- `fix_recipe` : instruction complète pour le correcteur :
  (1) table exacte et champ(s) à modifier,
  (2) mécanisme précis — pour GROUP BY/agrégat : "dupliquer N lignes avec [col]='[valeur]'" JAMAIS "ajouter des valeurs variables",
  (3) valeurs concrètes avec compte par groupe (ex: "'2016-01-02' × 3, '2016-01-03' × 2, '2016-01-01' × 1"),
  (4) effet attendu (ex: "→ COUNT ∈ {{1,2,3}} → STDDEV > 0").
- `affected_tables` : noms des tables dont les données doivent être corrigées
- `affected_ctes` : CTEs impactées par le problème"""

    llm = make_llm()
    structured_llm = llm.with_structured_output(DiagnosticBlock)
    try:
        logger.diag("[diagnostic] appel LLM ciblé bad_data")
        diag: DiagnosticBlock = await structured_llm.ainvoke(prompt)
        logger.diag(
            "[diagnostic] root_cause=%r\n  data_issue=%r\n  fix_recipe=%r\n  fix_summary=%r\n  affected_tables=%s\n  affected_ctes=%s",
            diag.root_cause,
            diag.data_issue,
            diag.fix_recipe,
            diag.fix_summary,
            diag.affected_tables,
            diag.affected_ctes,
        )
        return diag
    except Exception as e:
        logger.diag("[diagnostic] ERREUR: %s", e)
        return None


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


async def _regenerate_assertion(
    original: Dict[str, Any],
    error: str,
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
) -> Optional[Dict[str, Any]]:
    """
    Demande au LLM de corriger une assertion dont l'exécution a produit une erreur.
    Retourne un nouveau dict {"description": ..., "sql": ...} ou None en cas d'échec.
    """
    schema_lines = [f"  - `{col}`: {dtype}" for col, dtype in result_df.dtypes.items()]
    schema_str = "\n".join(schema_lines) if schema_lines else "  (aucune colonne)"

    # ── System : rôle + index des sections + règles (préfixe stable → cacheable) ──
    system_content = """Tu es un expert en tests SQL DuckDB dbt-style. Une assertion a \
échoué à l'exécution ; tu dois la réécrire pour qu'elle soit valide en DuckDB.

Le message suivant contient ces sections, délimitées par des balises :
- `<test_context>` : la description métier du scénario testé.
- `<result_schema>` : le schéma exact de la table `__result__` (colonnes + types).
- `<query>` : la requête SQL testée.
- `<input_data>` : les données d'entrée injectées dans DuckDB.
- `<broken_assertion>` : l'assertion fautive et l'erreur qu'elle a produite.
- `<task>` : ce que tu dois produire.

**Règles de réécriture :**
- Corrige UNIQUEMENT le SQL pour qu'il soit valide en DuckDB.
- L'assertion doit retourner 0 ligne si OK, des lignes si KO (convention dbt-style).
- INTERDIT absolu : ne référence AUCUNE table en dehors de `__result__`. Si l'assertion
  originale référençait une autre table (source ou suffixée), réécris-la pour n'utiliser que
  `__result__` et ses colonnes de `<result_schema>`.
- Ne jamais référencer un alias SELECT dans le WHERE — utiliser une sous-requête.
- Recopie la `description` d'origine À L'IDENTIQUE (en français, courte) : seul le SQL était cassé.

Réponds UNIQUEMENT avec un objet JSON (aucun texte autour) :
{"description": "...", "sql": "SELECT ..."}"""

    # ── Human : sections balisées contexte → tables → SQL → input → assertion fautive → ask ──
    human_content = f"""<test_context>
{test_description}
</test_context>

<result_schema>
{schema_str}
</result_schema>

<query>
```sql
{duckdb_sql}
```
</query>

<input_data>
{test_data}
</input_data>

<broken_assertion>
Description : {original.get("description", "")}
SQL :
```sql
{original.get("sql", "")}
```
Erreur : {error}
</broken_assertion>

<task>
Réécris l'assertion (description + sql valide DuckDB) en respectant les règles du message système.
</task>"""

    llm = make_llm()
    try:
        logger.diag(
            "[regen_assertion] assertion à corriger: %r",
            original.get("description", ""),
        )
        logger.diag("[regen_assertion] erreur: %s", error)
        result = await llm.ainvoke(
            [SystemMessage(content=system_content), HumanMessage(content=human_content)]
        )
        content = normalize_llm_content(result.content)
        logger.diag("[regen_assertion] réponse brute:\n%s", content[:500])
        json_match = re.search(r"\{[\s\S]*\}", content)
        if json_match:
            parsed = loads_lenient_json(json_match.group())
            if isinstance(parsed, dict) and parsed.get("sql"):
                # Seul le SQL était cassé : on conserve la description métier d'origine
                # (évite une réécriture en anglais ou verbeuse par le LLM).
                parsed["description"] = original.get("description", "") or parsed.get(
                    "description", ""
                )
                return parsed
    except Exception as e:
        logger.diag("[regen_assertion] ERREUR: %s", e)
    return None


_SUFFIXED_TABLE_RE = re.compile(
    r'"[^"]+_[0-9a-f]{8}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{4}_[0-9a-f]{12}[^"]*"'
)


def _assertion_references_source_tables(sql: str) -> bool:
    """Return True if the assertion SQL contains session-suffixed table names (UUID pattern).
    These are invalid outside the current DuckDB session and must be rejected."""
    return bool(_SUFFIXED_TABLE_RE.search(sql))


async def _evaluate_assertions_with_retry(
    assertions: List[Dict[str, Any]],
    view_name: str,
    con,
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
) -> List[Dict[str, Any]]:
    """
    Évalue les assertions et retente la régénération (jusqu'à REGEN_ASSERTION_LIMIT fois)
    de celles qui produisent une erreur d'exécution (pas juste un échec métier).
    """
    logger.diag("[assertion_retry] évaluation de %s assertion(s)", len(assertions))
    results = _evaluate_assertions(assertions, view_name, con)
    logger.diag(
        "[assertion_retry] résultats initiaux: %s",
        [{"passed": r.get("passed"), "error": bool(r.get("error"))} for r in results],
    )

    for attempt in range(REGEN_ASSERTION_LIMIT):
        errored_indices = [i for i, r in enumerate(results) if r.get("error")]
        if not errored_indices:
            break
        logger.diag(
            "[assertion_retry] tentative %s/%s — %s assertion(s) en erreur",
            attempt + 1,
            REGEN_ASSERTION_LIMIT,
            len(errored_indices),
        )
        for i in errored_indices:
            new_assertion = await _regenerate_assertion(
                original=results[i],
                error=results[i]["error"],
                duckdb_sql=duckdb_sql,
                test_data=test_data,
                result_df=result_df,
                test_description=test_description,
            )
            if new_assertion and not _assertion_references_source_tables(
                new_assertion.get("sql", "")
            ):
                new_eval = _evaluate_assertions([new_assertion], view_name, con)
                results[i] = new_eval[0]
            elif new_assertion:
                logger.diag(
                    "[assertion_retry] assertion régénérée rejetée — référence table non-__result__: %s",
                    new_assertion.get("sql", "")[:200],
                )

    return results


async def _fix_logically_failing_assertions(
    assertion_results: List[Dict[str, Any]],
    view_name: str,
    con,
    duckdb_sql: str,
    test_data: list,
    result_df,
    test_description: str,
) -> List[Dict[str, Any]]:
    """
    Pour les assertions qui échouent logiquement (passed=False, sans erreur SQL),
    demande au LLM si l'assertion elle-même est incorrecte. Si oui, la régénère
    et la réévalue une fois. Appelée uniquement lors de la génération initiale.
    """
    schema_lines = [f"  - `{col}`: {dtype}" for col, dtype in result_df.dtypes.items()]
    schema_str = "\n".join(schema_lines) if schema_lines else "  (aucune colonne)"
    sample = result_df.head(5).to_dict(orient="records")
    results = list(assertion_results)

    failing_indices = [
        i for i, r in enumerate(results) if not r.get("passed") and not r.get("error")
    ]
    logger.diag(
        "[assertion_fixer] %s assertion(s) logiquement échouée(s) sur %s",
        len(failing_indices),
        len(assertion_results),
    )
    if not failing_indices:
        return results

    # ── System : rôle + index des sections + règles. Un seul appel traite TOUTES les
    #    assertions échouées : le contexte commun (schema/query/input/sample) n'est envoyé
    #    qu'une fois et le LLM décide/corrige chacune via son `id` local (#0, #1, …) — au
    #    lieu d'un appel séquentiel par assertion qui re-postait ce contexte à chaque tour. ──
    system_content = """Tu es un expert en tests SQL DuckDB dbt-style. Tu viens de générer \
plusieurs assertions qui échouent (chacune retourne des lignes alors qu'elle devrait en \
retourner 0). Pour CHACUNE, tu dois déterminer si elle est logiquement correcte, ou si tu as \
fait une erreur dans sa logique.

Le message suivant contient ces sections, délimitées par des balises :
- `<test_context>` : la description métier du scénario testé.
- `<result_schema>` : le schéma exact de la table `__result__` (colonnes + types).
- `<query>` : la requête SQL testée.
- `<input_data>` : les données d'entrée injectées dans DuckDB.
- `<result_sample>` : des exemples du résultat réel.
- `<failing_assertions>` : les assertions qui échouent, chacune identifiée par un `id` (#0, #1, …),
  avec son SQL et les lignes qu'elle remonte.
- `<task>` : ce que tu dois produire.

**Décision attendue pour chaque assertion :** est-elle logiquement correcte par rapport au
résultat réel, ou as-tu fait une erreur dans sa formulation (mauvaise valeur attendue, mauvaise
colonne, condition inversée, etc.) ?
- Si l'assertion est **correcte** et le test échoue vraiment → `{"id": <id>, "correct": true}`.
  ⚠️ C'est aussi le cas si le **résultat réel ne correspond pas** à ce que le test annonçait
  (la donnée d'entrée ou la description sont en cause, pas l'assertion) : laisse-la en échec,
  ne fabrique JAMAIS une assertion qui « passe » artificiellement.
- Si l'assertion est **incorrecte** (tu as fait une erreur de logique) → régénère-la en
  fournissant une **`expected_condition` POSITIVE** (l'affirmation métier qui doit être VRAIE
  sur chaque ligne) : `{"id": <id>, "correct": false, "description": "...", "expected_condition": "..."}`

**Règles de l'`expected_condition` :**
- Condition booléenne POSITIVE exprimée directement (jamais sa négation). MockSQL la négocie
  lui-même pour produire la requête de validation.
- INTERDIT : tout `!=`, `<>`, `NOT IN`, `NOT (...)`, `IS NULL`, ou une clause `SELECT`/`WHERE`
  de tête — écris seulement l'expression booléenne (ex. `montant > 0`, `date = '2026-01-02'`).
- INTERDIT : toute clause qui se neutralise elle-même (ex. `x = 2 AND (SELECT COUNT(*) … x = 2) = 0`) :
  c'est une assertion creuse qui ne teste rien.
- Utilise UNIQUEMENT les colonnes de `<result_schema>` (casse exacte). Pour une valeur relative,
  une sous-requête sur `__result__` uniquement. Jamais d'alias SELECT dans une condition.

**Règle de la `description` (si tu régénères une assertion) :** phrase EN FRANÇAIS, courte
(max 12 mots), en langage métier — jamais en anglais, sans noms de colonnes/CTEs ni mots-clés SQL.

Réponds UNIQUEMENT avec un objet JSON (aucun texte autour), une décision par assertion :
{"decisions": [{"id": 0, "correct": true}, {"id": 1, "correct": false, "description": "...", "expected_condition": "..."}]}"""

    # ── Bloc <failing_assertions> : une entrée par assertion échouée, indexée par `id` local. ──
    blocks = []
    for local_id, i in enumerate(failing_indices):
        a = results[i]
        failing_rows = a.get("failing_rows", [])
        logger.diag(
            "[assertion_fixer] #%s (idx %s): %r", local_id, i, a.get("description", "")
        )
        blocks.append(
            f"""#{local_id}
Description : {a.get("description", "")}
SQL :
```sql
{a.get("sql", "")}
```
Lignes remontées (violations détectées) :
{json.dumps(failing_rows[:10], ensure_ascii=False, default=str)}"""
        )
    failing_block = "\n\n".join(blocks)

    human_content = f"""<test_context>
{test_description}
</test_context>

<result_schema>
{schema_str}
</result_schema>

<query>
```sql
{duckdb_sql}
```
</query>

<input_data>
{test_data}
</input_data>

<result_sample>
{json.dumps(sample, ensure_ascii=False, default=str)}
</result_sample>

<failing_assertions>
{failing_block}
</failing_assertions>

<task>
Pour chaque assertion (#0 … #{len(failing_indices) - 1}), décide si elle est correcte ou erronée,
et réponds selon le format du message système (un objet `decisions` listant une entrée par `id`).
</task>"""

    llm = make_llm()
    try:
        result = await llm.ainvoke(
            [
                SystemMessage(content=system_content),
                HumanMessage(content=human_content),
            ]
        )
        content = normalize_llm_content(result.content)
        logger.diag("[assertion_fixer] réponse LLM:\n%s", content[:800])
        json_match = re.search(r"\{[\s\S]*\}", content)
        if not json_match:
            return results
        parsed = loads_lenient_json(json_match.group())
        decisions = parsed.get("decisions") if isinstance(parsed, dict) else None
        if not isinstance(decisions, list):
            return results

        for dec in decisions:
            if not isinstance(dec, dict) or dec.get("correct"):
                continue
            local_id = dec.get("id")
            if not isinstance(local_id, int) or not (
                0 <= local_id < len(failing_indices)
            ):
                continue
            target = failing_indices[local_id]
            new_cond = (dec.get("expected_condition") or "").strip()
            # Garde 1 — condition positive valide. Sinon (vide, négative, SQL brut) on garde
            # l'assertion d'origine en échec : pas de blanchiment via une forme non maîtrisée.
            if not _is_valid_positive_condition(new_cond):
                logger.diag(
                    "[assertion_fixer] #%s rejeté : expected_condition invalide/vide %r",
                    local_id,
                    new_cond,
                )
                continue
            # Préserve un scope existant (ou un nouveau fourni par le fixer) : sans cela
            # une assertion scopée serait « réparée » en une forme non scopée potentiellement
            # vacuité. La couverture du scope est revalidée par _evaluate_assertions (Garde 2).
            new_scope = (dec.get("scope") or results[target].get("scope") or "").strip()
            new_assertion = {
                "description": dec.get("description", results[target]["description"]),
                "expected_condition": new_cond,
                **({"scope": new_scope} if new_scope else {}),
                "sql": _assertion_sql_from_condition(new_cond, new_scope or None),
            }
            new_eval = _evaluate_assertions([new_assertion], view_name, con)
            # Garde 2 — anti-blanchiment : si la réécriture échoue toujours (ou erreur), le
            # problème n'est pas la logique de l'assertion (donnée/description en cause) →
            # on conserve l'assertion d'origine en échec plutôt que de la remplacer.
            if not new_eval[0].get("passed"):
                logger.diag(
                    "[assertion_fixer] #%s rejeté : la réécriture échoue toujours (pas un fix de logique)",
                    local_id,
                )
                continue
            results[target] = new_eval[0]
    except Exception:
        pass

    return results


def _determine_global_status(all_tests_results: List[Dict[str, Any]]) -> str:
    """
    Détermine le statut global en fonction des résultats de tous les tests.
    Seul le premier test (cas standard sans instruction utilisateur) peut déclencher
    un retry : si son résultat est vide, on renvoie 'empty_results'.
    Les tests suivants (avec instruction utilisateur) peuvent légitimement être vides.
    Une erreur DuckDB (parsing, binder…) n'est pas corrigeable par les données : on
    renvoie 'error' pour stopper les boucles de retry.
    """
    if not all_tests_results:
        return "complete"
    first = all_tests_results[0]
    if first.get("status") == "error":
        return "error"
    if first.get("status") == "bad_data_error":
        return "bad_data_error"
    if first.get("status") == "empty_results":
        return "empty_results"
    return "complete"


async def format_result(res: DataFrame) -> str:
    """
    Convertit le DataFrame en JSON (orientation = records).
    Retourne une chaîne JSON.
    """
    format_res = res.to_json(orient="records", date_format="iso", date_unit="s")
    return str(format_res)
