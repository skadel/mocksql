"""Régression root-cause spider2-snow — un mort-né CLI ne doit plus être silencieux.

`mocksql generate` écrivait `[OK] N test case(s) écrits` + rc=0 même quand la boucle
de génération sortait en échec (`empty_results`/`error`/`bad_data_error`) : le stub
`FAILED_AUTO_GEN` du circuit-breaker partait sur le canal `examples` que la CLI ne lit
pas. `mark_failed_cases` dérive le marquage directement du `status` des cas extraits —
couvrant AUSSI les échecs hors circuit-breaker — sans jamais vider les données.

Second volet (diagnostic 12/07, 66/110 modèles) : le fichier persisté portait
`verdict=null` — le verdict « Insuffisant » de l'évaluateur n'existe qu'en message
EVALUATION + state sur ces chemins, jamais fusionné dans le RESULTS que la CLI lit.
`mark_failed_cases` pose donc aussi un verdict argumenté (explication reprise du
dernier message EVALUATION du test), en épargnant le PASS « vide intentionnel »
(status=empty_results MAIS verdict Bon/Excellent + assertion sentinelle).
"""

from langchain_core.messages import AIMessage

from cli.generate import mark_failed_cases
from utils.msg_types import MsgType


def _eval_msg(test_index: str, content: str, **extra_kwargs) -> AIMessage:
    return AIMessage(
        content=content,
        additional_kwargs={
            "type": MsgType.EVALUATION,
            "test_index": test_index,
            **extra_kwargs,
        },
    )


def _cases():
    return [
        {
            "test_index": "1",
            "test_name": "nominal",
            "status": "complete",
            "tags": ["Business logic"],
            "data": {"t": [{"a": 1}]},
        },
        {
            "test_index": "2",
            "test_name": "manche",
            "status": "empty_results",
            "tags": ["Business logic"],
            "data": {"t": [{"a": "M001"}]},
        },
        {
            "test_index": "3",
            "test_name": "flatten",
            "status": "error",
            "error": 'Parser Error: syntax error at or near "JOIN"',
            "data": {"t": []},
        },
    ]


def test_failed_cases_tagged_and_returned():
    cases = _cases()
    failed = mark_failed_cases(cases)

    assert [tc["test_index"] for tc in failed] == ["2", "3"]
    for tc in failed:
        assert "FAILED_AUTO_GEN" in tc["tags"]
        assert "MANUAL_REVIEW_NEEDED" in tc["tags"]
    # Le cas sain n'est pas touché.
    assert cases[0]["tags"] == ["Business logic"]


def test_data_never_destroyed():
    """Contrairement au stub du circuit-breaker, le marquage ne vide PAS les données :
    elles restent exploitables pour une correction/le mode additif."""
    cases = _cases()
    mark_failed_cases(cases)
    assert cases[1]["data"] == {"t": [{"a": "M001"}]}


def test_marking_is_idempotent():
    cases = _cases()
    mark_failed_cases(cases)
    mark_failed_cases(cases)
    assert cases[1]["tags"].count("FAILED_AUTO_GEN") == 1
    assert cases[1]["tags"].count("MANUAL_REVIEW_NEEDED") == 1


def test_dead_cases_get_argued_verdict():
    """Le symptôme d'origine : verdict=null dans le fichier commité. Un mort-né doit
    porter un verdict Insuffisant + une cause typée, comme les tests évalués."""
    cases = _cases()
    failed = mark_failed_cases(cases)

    by_index = {tc["test_index"]: tc for tc in failed}
    assert by_index["2"]["verdict"] == "Insuffisant"
    assert by_index["2"]["reason_type"] == "bad_data"
    assert by_index["3"]["verdict"] == "Insuffisant"
    assert by_index["3"]["reason_type"] == "execution_error"
    # Le cas sain ne reçoit PAS de verdict fabriqué.
    assert "verdict" not in cases[0]


def test_explanation_taken_from_last_evaluation_message():
    cases = [
        {
            "test_index": "1",
            "status": "empty_results",
            "failing_cte": "approved_repos",
            "tags": [],
        }
    ]
    messages = [
        _eval_msg("1", "**Insuffisant** — vieux diagnostic", intermediate=True),
        _eval_msg("1", "**Insuffisant** — Les données ne satisfont pas la jointure."),
        _eval_msg("9", "**Insuffisant** — diagnostic d'un AUTRE test"),
    ]
    mark_failed_cases(cases, messages)
    # Dernier message EVALUATION de CE test, préfixe « **verdict** — » retiré.
    assert (
        cases[0]["evaluation_explanation"]
        == "Les données ne satisfont pas la jointure."
    )


def test_explanation_falls_back_on_error_message():
    """Statut `error` : aucun message EVALUATION n'existe (route directe history_saver)
    — l'explication doit quand même être renseignée depuis l'erreur d'exécution."""
    cases = _cases()
    mark_failed_cases(cases, [])
    dead_error = cases[2]
    explanation = dead_error.get("evaluation_explanation") or ""
    assert "Parser Error" in explanation


def test_intentional_empty_pass_is_not_marked():
    """Le PASS « vide intentionnel » garde status=empty_results AVEC un verdict
    Bon/Excellent (+ assertion sentinelle) : il ne doit JAMAIS être marqué mort-né."""
    cases = [
        {
            "test_index": "1",
            "status": "empty_results",
            "verdict": "Bon",
            "tags": ["Cas limites"],
            "assertion_results": [{"description": "vide attendu", "passed": True}],
        }
    ]
    failed = mark_failed_cases(cases, [])
    assert failed == []
    assert cases[0]["verdict"] == "Bon"
    assert "FAILED_AUTO_GEN" not in cases[0]["tags"]
    assert "evaluation_explanation" not in cases[0]


def test_previously_marked_dead_case_stays_marked():
    """Idempotence du verdict : un mort-né relu (mode additif) porte déjà
    verdict=Insuffisant — il reste marqué, sans doublonner ni écraser l'explication."""
    cases = [
        {
            "test_index": "1",
            "status": "empty_results",
            "verdict": "Insuffisant",
            "evaluation_explanation": "explication d'origine",
            "tags": ["FAILED_AUTO_GEN", "MANUAL_REVIEW_NEEDED"],
        }
    ]
    failed = mark_failed_cases(cases, [])
    assert len(failed) == 1
    assert cases[0]["evaluation_explanation"] == "explication d'origine"
    assert cases[0]["tags"].count("FAILED_AUTO_GEN") == 1
