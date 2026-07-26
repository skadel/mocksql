import json
import re
from typing import Any

# Antislash qui n'introduit PAS un échappement JSON valide (les seuls légaux
# après `\` sont : " \ / b f n r t u). Les LLM produisent fréquemment du SQL
# avec des apostrophes échappées « à la C » (`Caisse d\'Epargne`) à l'intérieur
# d'une string JSON — ce qui est un échappement illégal et fait planter
# json.loads avec « Invalid \escape ».
_INVALID_JSON_ESCAPE_RE = re.compile(r'\\(?![\\"/bfnrtu])')


def normalize_llm_content(content) -> str:
    """Flatten LangChain multi-part content blocks into a plain string."""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "".join(
            p
            if isinstance(p, str)
            else (p.get("text", "") if isinstance(p, dict) else "")
            for p in content
        )
    return str(content)


def loads_lenient_json(raw: str) -> Any:
    """Parse du JSON produit par un LLM, tolérant aux échappements illégaux.

    Tente d'abord `json.loads` standard. En cas d'échec sur un échappement
    invalide (le cas dominant : `\\'` dans du SQL embarqué), retire les antislash
    parasites et retente une seule fois. Lève l'exception d'origine si la reprise
    échoue aussi — on ne masque pas un JSON réellement cassé.
    """
    try:
        return json.loads(raw)
    except json.JSONDecodeError:
        repaired = _INVALID_JSON_ESCAPE_RE.sub("", raw)
        return json.loads(repaired)


def is_vertex_permission_error(exc: Exception) -> bool:
    return classify_vertex_access_error(exc) is not None


def classify_vertex_access_error(exc: Exception) -> str | None:
    """Return a stable, credential-safe category for common Vertex failures."""
    error = str(exc).upper()
    if "DEFAULTCREDENTIALSERROR" in error or "APPLICATION DEFAULT CREDENTIALS" in error:
        return "adc_missing"
    if (
        "VERTEX AI API HAS NOT BEEN USED" in error
        or "AIPLATFORM.GOOGLEAPIS.COM" in error
    ):
        return "api_disabled"
    if "PERMISSION_DENIED" in error or "BILLING_DISABLED" in error:
        if "AIPLATFORM.USER" in error:
            return "iam_role_missing"
        if (
            "PUBLISHER MODEL" in error
            or "GENERATIVE LANGUAGE" in error
            or "MODEL" in error
        ):
            return "model_access_denied"
        return "permission_denied"
    return None


def format_vertex_permission_message(
    model_name: str, exc: Exception | None = None
) -> str:
    category = classify_vertex_access_error(exc) if exc else None
    guidance = {
        "adc_missing": "Configurez les Application Default Credentials avec `gcloud auth application-default login` ou GOOGLE_APPLICATION_CREDENTIALS.",
        "api_disabled": "Activez l'API Vertex AI (`aiplatform.googleapis.com`) pour le projet Vertex.",
        "iam_role_missing": "Accordez le rôle `roles/aiplatform.user` au compte qui exécute MockSQL.",
        "model_access_denied": "Vérifiez que Gemini et ce modèle sont autorisés pour le projet, la région et l'organisation.",
    }
    if category in guidance:
        return f"Erreur d'accès Vertex AI ({category}) pour « {model_name} ».\n• {guidance[category]}"
    return (
        f"Erreur d'accès au modèle LLM (PERMISSION_DENIED).\n"
        f"• Vérifiez que le modèle « {model_name} » est accessible dans votre organisation.\n"
        f"• Vérifiez vos permissions IAM sur Vertex AI (rôle minimum requis : AI Platform Developer)."
    )
