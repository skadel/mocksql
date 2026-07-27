"""Vertex errors must be actionable without exposing credential contents."""

import pytest
from langchain_core.messages import AIMessage

from cli.generate import cli_state_error_message
from utils.llm_errors import (
    classify_vertex_access_error,
    format_vertex_permission_message,
    is_vertex_permission_error,
)


@pytest.mark.parametrize(
    ("raw", "category", "expected"),
    [
        (
            "DefaultCredentialsError: Application Default Credentials",
            "adc_missing",
            "Application Default Credentials",
        ),
        (
            "Vertex AI API has not been used in project",
            "api_disabled",
            "Activez l'API Vertex AI",
        ),
        (
            "PERMISSION_DENIED: missing roles/aiplatform.user",
            "iam_role_missing",
            "roles/aiplatform.user",
        ),
        (
            "PERMISSION_DENIED: Publisher Model access denied",
            "model_access_denied",
            "Gemini",
        ),
    ],
)
def test_vertex_access_errors_are_classified_and_actionable(
    raw: str, category: str, expected: str
) -> None:
    exc = RuntimeError(raw)

    assert classify_vertex_access_error(exc) == category
    assert is_vertex_permission_error(exc)
    assert expected in format_vertex_permission_message("gemini-2.5-flash", exc)


def test_non_vertex_error_is_not_misclassified() -> None:
    assert classify_vertex_access_error(RuntimeError("network timeout")) is None
    assert not is_vertex_permission_error(RuntimeError("network timeout"))


def test_vertex_message_covers_all_recovery_paths_without_leaking_exception() -> None:
    secret_marker = "token=super-sensitive-value"
    message = format_vertex_permission_message(
        "gemini-2.5-flash",
        RuntimeError(f"PERMISSION_DENIED: {secret_marker}"),
    )

    assert "Appel Vertex AI refusé" in message
    assert "VERTEX_PROJECT" in message
    assert "gcloud auth application-default login" in message
    assert "GOOGLE_APPLICATION_CREDENTIALS" in message
    assert "aiplatform.googleapis.com" in message
    assert "roles/aiplatform.user" in message
    assert "llm.provider: openai" in message
    assert "OPENAI_API_KEY" in message
    assert secret_marker not in message


def test_cli_displays_vertex_message_instead_of_internal_error_code() -> None:
    message = format_vertex_permission_message(
        "gemini-2.5-flash",
        RuntimeError("PERMISSION_DENIED: missing roles/aiplatform.user"),
    )
    final_state = {
        "error": "llm_permission_denied",
        "messages": [
            AIMessage(
                content=message,
                additional_kwargs={"type": "error"},
            )
        ],
    }

    displayed = cli_state_error_message(final_state)

    assert displayed == message
    assert displayed != "llm_permission_denied"
