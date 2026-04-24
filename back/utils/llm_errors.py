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


def is_vertex_permission_error(exc: Exception) -> bool:
    err_str = str(exc)
    return "PERMISSION_DENIED" in err_str or "BILLING_DISABLED" in err_str


def format_vertex_permission_message(model_name: str) -> str:
    return (
        f"Erreur d'accès au modèle LLM (PERMISSION_DENIED).\n"
        f"• Vérifiez que le modèle « {model_name} » est accessible dans votre organisation.\n"
        f"• Vérifiez vos permissions IAM sur Vertex AI (rôle minimum requis : AI Platform Developer)."
    )
