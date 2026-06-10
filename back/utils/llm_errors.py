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
    err_str = str(exc)
    return "PERMISSION_DENIED" in err_str or "BILLING_DISABLED" in err_str


def format_vertex_permission_message(model_name: str) -> str:
    return (
        f"Erreur d'accès au modèle LLM (PERMISSION_DENIED).\n"
        f"• Vérifiez que le modèle « {model_name} » est accessible dans votre organisation.\n"
        f"• Vérifiez vos permissions IAM sur Vertex AI (rôle minimum requis : AI Platform Developer)."
    )
