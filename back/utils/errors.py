# errors.py
import asyncio
import json
import re
import uuid
from typing import Any, Dict, Optional

from fastapi import HTTPException as FastAPIHTTPException
from google.api_core.exceptions import BadRequest, Forbidden, NotFound
from langchain_core.messages import AIMessage, HumanMessage, BaseMessage
from sqlglot import ParseError
from sqlglot.errors import OptimizeError
from starlette import status

from app.exceptions.exceptions import TableNotFoundException
from utils.prompt_utils import escape_unescaped_placeholders


def get_line_number(error_message):
    # Regular expressions to match different line number patterns
    pattern1 = re.compile(r"Line (\d+)")
    pattern2 = re.compile(r"\[(\d+):\d+]")

    match1 = pattern1.search(error_message)
    if match1:
        return int(match1.group(1))

    match2 = pattern2.search(error_message)
    if match2:
        return int(match2.group(1))

    return None


def strip_table_suffix(table_name: str) -> str:
    # Supprime un suffixe de type UUID ou numérique (ex : _49d7_925a_811675c081ed ou _13043563)
    return re.sub(r"(_[a-f0-9]{4,})(_[a-f0-9]{4,}){1,}$", "", table_name)


def _solver_error_payload(msg: str) -> Dict[str, Any]:
    """Payload générique de type solver_messages -> HumanMessage(error)."""
    return {
        "solver_messages": [
            HumanMessage(content=msg, additional_kwargs={"type": "error"})
        ]
    }


def _solver_error(msg: str) -> Dict[str, Any]:
    return {
        "solver_messages": [
            HumanMessage(content=msg, additional_kwargs={"type": "error"})
        ]
    }


def safe_last_parent_id(state: dict) -> Optional[str]:
    """
    Essaie d'extraire l'id du dernier message dans state["messages"].
    Gère les cas: objets LangChain (BaseMessage), dicts, None, liste vide.
    """
    messages = state.get("messages")
    if not messages:
        return None

    last = messages[-1]

    # Cas LangChain BaseMessage
    if isinstance(last, BaseMessage):
        mid = getattr(last, "id", None)
        if mid:
            return mid
        add_kwargs = getattr(last, "additional_kwargs", None)
        if isinstance(add_kwargs, dict):
            return add_kwargs.get("id") or add_kwargs.get("message_id")

    # Cas dict
    if isinstance(last, dict):
        add_kwargs = last.get("additional_kwargs") or {}
        return last.get("id") or add_kwargs.get("id") or add_kwargs.get("message_id")

    return None


def _messages_for_sql_route(
    *, code: str, parent: Optional[str], state: dict, user_text: str
) -> Dict[str, Any]:
    """Fabrique le payload 'messages' utilisé quand route contient 'sql_code'."""
    error_id = str(uuid.uuid4())
    return {
        "messages": [
            AIMessage(
                content=code,
                id=error_id,
                additional_kwargs={
                    "type": "error_sql",
                    "parent": parent,
                    "is_analysis": state.get("is_analysis"),
                },
            ),
            HumanMessage(
                content=user_text,
                additional_kwargs={
                    "type": "error",
                    "parent": error_id,
                    "is_analysis": state.get("is_analysis"),
                },
            ),
        ]
    }


def handle_compile_phase_exceptions(
    *,
    exc: Exception,
    code: str,
    route: str,
    parent: str,
    state: dict,
) -> Dict[str, Any]:
    """
    Gère les exceptions levées pendant la phase 'compile_query'.
    Retourne un dict 'payload' prêt à être renvoyé par l'endpoint.
    """
    # BadRequest
    if isinstance(exc, BadRequest):
        line_number = get_line_number(repr(exc))
        escaped_error = escape_unescaped_placeholders(repr(exc))

        # Cas route sql_code -> renvoyer le code et le message utilisateur
        if "sql_code" in route:
            r = _messages_for_sql_route(
                code=code,
                parent=parent,
                state=state,
                user_text="Cette requête génère une erreur : " + escaped_error,
            )
            return {"error": escaped_error, **r}

        # Cas message d’agrégation
        if "neither grouped nor aggregated" in str(exc):
            return _solver_error_payload(
                f"please fix the following error {escaped_error}"
            )

        # Cas erreur avec n° de ligne
        if line_number:
            error_id = str(uuid.uuid4())
            user_msg = f"Erreur SQL à la ligne {line_number} : {escaped_error}"
            return {
                "status": "line_error",
                "line_number": line_number,
                "current_query": code,
                "compilation_error": repr(exc),
                "messages": [
                    AIMessage(
                        content=code,
                        id=error_id,
                        additional_kwargs={
                            "type": "error_sql",
                            "parent": parent,
                            "fixable": True,
                        },
                    ),
                    HumanMessage(
                        content=user_msg,
                        additional_kwargs={
                            "type": "error",
                            "parent": error_id,
                            "fixable": True,
                        },
                    ),
                ],
            }

        # Fallback générique
        return _solver_error_payload(f"please fix the following error {escaped_error}")

    # TableNotFoundException
    if isinstance(exc, TableNotFoundException):
        escaped = escape_unescaped_placeholders(repr(exc))
        if "sql_code" in route:
            r = _messages_for_sql_route(
                code=code,
                parent=parent,
                state=state,
                user_text="Cette requête génère une erreur : " + escaped,
            )
            return {"error": escaped, **r}

        return _solver_error_payload(escaped)

    # NotFound (BigQuery)
    if isinstance(exc, NotFound):
        error_id = str(uuid.uuid4())
        error_message = repr(exc)
        escaped_error = escape_unescaped_placeholders(error_message)

        match = re.search(
            r"Table [^:]+:(?P<dataset>[^.]+)\.(?P<table>[^ ]+)", error_message
        )
        if match:
            table = match.group("table")
            base_table = strip_table_suffix(table)
            user_message = (
                f"La table `{base_table}` n'est pas définie. "
                f"Veuillez la créer ou vérifier son nom."
            )
        else:
            user_message = "Cette requête génère une erreur : " + escaped_error

        return {
            "error": escaped_error,
            "messages": [
                AIMessage(
                    content=code,
                    id=error_id,
                    additional_kwargs={
                        "type": "error_sql",
                        "parent": parent,
                        "is_analysis": state.get("is_analysis"),
                    },
                ),
                HumanMessage(
                    content=user_message,
                    additional_kwargs={
                        "type": "error",
                        "parent": error_id,
                        "is_analysis": state.get("is_analysis"),
                    },
                ),
            ],
        }

    # Forbidden (droits BigQuery insuffisants — ex. bigquery.jobs.create)
    if isinstance(exc, Forbidden):
        return {
            "status": "permission_error",
            "messages": [
                HumanMessage(
                    content=(
                        "Accès refusé par BigQuery (droits insuffisants).\n"
                        "Votre compte doit disposer des rôles IAM suivants sur le projet GCP :\n"
                        "  • BigQuery Data Viewer  (roles/bigquery.dataViewer)\n"
                        "  • BigQuery User         (roles/bigquery.user)\n\n"
                        "Ajout via gcloud :\n"
                        "  gcloud projects add-iam-policy-binding <PROJECT_ID> \\\n"
                        "    --member='user:<votre-email>' \\\n"
                        "    --role='roles/bigquery.dataViewer'\n"
                        "  gcloud projects add-iam-policy-binding <PROJECT_ID> \\\n"
                        "    --member='user:<votre-email>' \\\n"
                        "    --role='roles/bigquery.user'\n\n"
                        "Ou dans la console GCP : IAM & Admin → IAM → Accorder l'accès.\n"
                        "Consultez le README (section Permissions IAM) pour les instructions complètes."
                    ),
                    additional_kwargs={"type": "error"},
                )
            ],
        }

    # Exception générique
    escaped = escape_unescaped_placeholders(repr(exc))
    return _solver_error_payload(escaped)


def handle_post_compile_exceptions(*, exc: Exception, code: str) -> Dict[str, Any]:
    """
    Gère les exceptions de la phase post-compile
    (évaluation, split, parsing/optimisation).
    """
    if isinstance(exc, ParseError):
        line_number = get_line_number(repr(exc))
        if line_number:
            error_id = str(uuid.uuid4())
            escaped = escape_unescaped_placeholders(repr(exc))
            return {
                "status": "line_error",
                "line_number": line_number,
                "current_query": code,
                "compilation_error": repr(exc),
                "messages": [
                    AIMessage(
                        content=code,
                        id=error_id,
                        additional_kwargs={
                            "type": "error_sql",
                            "parent": None,
                            "fixable": True,
                        },
                    ),
                    HumanMessage(
                        content=f"Erreur SQL à la ligne {line_number} : {escaped}",
                        additional_kwargs={
                            "type": "error",
                            "parent": error_id,
                            "fixable": True,
                        },
                    ),
                ],
            }
        return _solver_error_payload(f"please fix the following error {repr(exc)}")

    if isinstance(exc, OptimizeError):
        return _solver_error_payload(f"please fix the following error {repr(exc)}")

    # Autre exception -> 500
    raise FastAPIHTTPException(
        status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail=str(exc)
    )


def handle_execution_exceptions(
    *,
    exc: Exception,
    state: dict,
    sql: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Gère les exceptions sur la phase d'exécution/run_query.
    Retourne un payload prêt à renvoyer par l’endpoint.
    """
    route = (state.get("route") or "").lower()
    parent_msg_id = safe_last_parent_id(state)
    escaped = escape_unescaped_placeholders(repr(exc))

    def _with_sql_or_solver(user_text: str) -> Dict[str, Any]:
        if "sql_code" in route and sql:
            return {
                "status": "failed",
                **_messages_for_sql_route(
                    code=sql, parent=parent_msg_id, state=state, user_text=user_text
                ),
            }
        return {"status": "failed", **_solver_error(user_text)}

    # Temps dépassé / annulation explicite
    if isinstance(
        exc, (asyncio.TimeoutError, TimeoutError)
    ) or exc.__class__.__name__ in {"QueryTimeoutError"}:
        return _with_sql_or_solver(
            "L'exécution a expiré. Réduisez le périmètre (filtres, LIMIT) ou optimisez la requête."
        )

    # HTTPException (souvent levée depuis run_query_bigquery)
    if isinstance(exc, FastAPIHTTPException):
        detail = exc.detail if isinstance(exc.detail, str) else json.dumps(exc.detail)
        return _with_sql_or_solver(f"Erreur d'exécution ({exc.status_code}) : {detail}")

    # Erreurs "fonctionnelles" connues
    if isinstance(exc, BadRequest):
        return _with_sql_or_solver("Erreur d'exécution : " + escaped)

    if isinstance(exc, TableNotFoundException):
        return _with_sql_or_solver(escaped)

    if isinstance(exc, NotFound):
        # Message plus pédagogique comme dans la phase compile
        m = re.search(r"Table [^:]+:(?P<dataset>[^.]+)\.(?P<table>[^ ]+)", repr(exc))
        if m:
            base_table = strip_table_suffix(m.group("table"))
            return _with_sql_or_solver(
                f"La table `{base_table}` est introuvable à l'exécution. Vérifiez son existence et vos droits."
            )
        return _with_sql_or_solver("Erreur d'exécution : " + escaped)

    if isinstance(exc, Forbidden):
        return _with_sql_or_solver(
            "Accès refusé par BigQuery (droits insuffisants).\n"
            "Vérifiez que votre compte dispose des rôles IAM requis :\n"
            "  • BigQuery Data Viewer  (roles/bigquery.dataViewer)\n"
            "  • BigQuery User         (roles/bigquery.user)\n"
            "Consultez le README (section Permissions IAM) pour les instructions."
        )

    # Fallback générique
    return _with_sql_or_solver("Erreur d'exécution : " + escaped)
