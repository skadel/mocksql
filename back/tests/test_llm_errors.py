"""Vertex errors must be actionable without exposing credential contents."""

import pytest

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
