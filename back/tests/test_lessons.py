"""Tests de la mémoire des leçons de correction (build_query/lessons.py).

Comportement visé :
  - les leçons s'accumulent par table ET par jointure, plafonnées à 3 par clé,
    dédupliquées, la plus récente en tête ;
  - le bloc injecté ne montre que les leçons des tables/jointures de la requête ;
  - la persistance respecte la source : "user" toujours écrite, "correction" écrite
    seulement à convergence (verdict suffisant).
"""

from build_query import lessons
from build_query.lessons import (
    LESSONS_CAP,
    add_lesson,
    format_lessons_block,
    join_key,
    make_lesson_entry,
    persist_pending_lessons,
)


# --- clés canoniques ---------------------------------------------------------


def test_join_key_is_order_independent_and_short():
    assert join_key("proj.ds.orders", "proj.ds.users") == "orders↔users"
    assert join_key("USERS", "orders") == join_key("orders", "users")


# --- add_lesson : dédup + plafond -------------------------------------------


def test_add_lesson_caps_at_three_per_table_newest_first():
    p: dict = {}
    for i in range(5):
        add_lesson(p, "table", "orders", f"règle {i}")
    rules = p["lessons"]["tables"]["orders"]
    assert len(rules) == LESSONS_CAP == 3
    # La plus récente en tête, les deux plus anciennes tombées.
    assert rules == ["règle 4", "règle 3", "règle 2"]


def test_add_lesson_caps_per_join_independently():
    p: dict = {}
    for i in range(4):
        add_lesson(p, "join", "orders↔users", f"j{i}")
    add_lesson(p, "table", "orders", "t0")
    assert len(p["lessons"]["joins"]["orders↔users"]) == 3
    # Le plafond joue par clé : la table garde la sienne.
    assert p["lessons"]["tables"]["orders"] == ["t0"]


def test_add_lesson_dedup_case_insensitive_moves_to_front():
    p: dict = {}
    add_lesson(p, "table", "orders", "Ne pas mettre de NULL")
    add_lesson(p, "table", "orders", "autre règle")
    add_lesson(p, "table", "orders", "ne pas mettre de null")  # doublon (casse)
    rules = p["lessons"]["tables"]["orders"]
    assert rules == ["ne pas mettre de null", "autre règle"]


def test_add_lesson_ignores_empty_or_bad_scope():
    p: dict = {}
    add_lesson(p, "table", "orders", "   ")
    add_lesson(p, "bogus", "orders", "x")
    assert p == {}


# --- format_lessons_block : pertinence ---------------------------------------


def _profile_with_lessons():
    p: dict = {}
    add_lesson(p, "table", "orders", "amounts toujours > 0")
    add_lesson(p, "table", "products", "category jamais NULL")
    add_lesson(p, "join", "orders↔users", "user_id doit exister dans users")
    return p


def test_format_block_only_used_tables_and_joins():
    p = _profile_with_lessons()
    used = [
        {"table": "proj.ds.orders", "used_columns": ["amount", "user_id"]},
        {"table": "proj.ds.users", "used_columns": ["id"]},
    ]
    block = format_lessons_block(p, used)
    assert "amounts toujours > 0" in block  # orders : utilisée
    assert "user_id doit exister" in block  # jointure : 2 côtés présents
    assert "products" not in block  # products non utilisée → exclue


def test_format_block_excludes_join_when_one_side_absent():
    p = _profile_with_lessons()
    used = [{"table": "proj.ds.orders", "used_columns": ["amount"]}]
    block = format_lessons_block(p, used)
    assert "amounts toujours > 0" in block
    assert "user_id doit exister" not in block  # users absente → jointure exclue


def test_format_block_empty_without_lessons():
    assert format_lessons_block({}, [{"table": "orders", "used_columns": []}]) == ""
    assert format_lessons_block(None, []) == ""


# --- injection dans le prompt de génération ----------------------------------


def test_generate_data_prompt_includes_lessons():
    """Une leçon sur une table utilisée doit apparaître dans le prompt de génération."""
    from build_query.prompt_tools import generate_data_prompt

    profile: dict = {}
    add_lesson(profile, "table", "banques", "code_banque jamais NULL pour réseau BP")
    used = [
        {"database": "MARKETING", "table": "banques", "used_columns": ["code_banque"]}
    ]

    msgs = generate_data_prompt(
        history=[],
        dialect="bigquery",
        format_instructions="FMT",
        used_columns=used,
        sql="SELECT code_banque FROM MARKETING.banques",
        profile=profile,
    ).format_messages()

    rendered = "\n".join(m.content for m in msgs)
    assert "code_banque jamais NULL pour réseau BP" in rendered
    assert "Leçons apprises" in rendered


# --- make_lesson_entry : clé canonique + source -----------------------------


def test_make_entry_join_computes_canonical_key():
    e = make_lesson_entry(
        "join", "user_id doit exister", left_table="users", right_table="proj.ds.orders"
    )
    assert e == {
        "scope": "join",
        "key": "orders↔users",
        "rule": "user_id doit exister",
        "source": "correction",
    }


def test_make_entry_table_with_user_source():
    e = make_lesson_entry(
        "table", "champ x jamais NULL pour BP", table="proj.ds.accounts", source="user"
    )
    assert e == {
        "scope": "table",
        "key": "accounts",
        "rule": "champ x jamais NULL pour BP",
        "source": "user",
    }


def test_make_entry_rejects_empty_or_incomplete():
    assert make_lesson_entry("table", "  ", table="orders") is None
    assert make_lesson_entry("join", "r", left_table="a") is None  # un seul côté


# --- persist_pending_lessons : gate de convergence par source ----------------


def _patch_profile_io(monkeypatch):
    saved = {}
    monkeypatch.setattr(lessons, "_load_model_profile", lambda: {}, raising=False)
    monkeypatch.setattr(
        lessons, "_save_model_profile", lambda p: saved.update(profile=p), raising=False
    )
    return saved


def _patch_verdict(monkeypatch, verdict):
    monkeypatch.setattr(
        lessons,
        "get_test",
        lambda *a, **k: {"test_cases": [{"test_uid": "u1", "verdict": verdict}]},
        raising=False,
    )


def test_persist_noop_without_pending(monkeypatch):
    saved = _patch_profile_io(monkeypatch)
    assert persist_pending_lessons({"pending_lessons": []}) is None
    assert saved == {}


def test_persist_skips_correction_lesson_when_not_converged(monkeypatch):
    saved = _patch_profile_io(monkeypatch)
    _patch_verdict(monkeypatch, "Insuffisant")
    state = {
        "session": "s1",
        "test_uid": "u1",
        "pending_lessons": [
            {"scope": "table", "key": "orders", "rule": "r", "source": "correction"}
        ],
    }
    assert persist_pending_lessons(state) is None
    assert saved == {}  # rien écrit


def test_persist_writes_user_lesson_even_without_convergence(monkeypatch):
    """Règle métier énoncée par l'utilisateur : persistée sans gate de convergence."""
    saved = _patch_profile_io(monkeypatch)
    _patch_verdict(monkeypatch, "Insuffisant")
    state = {
        "session": "s1",
        "test_uid": "u1",
        "pending_lessons": [
            {
                "scope": "table",
                "key": "accounts",
                "rule": "champ x jamais NULL",
                "source": "user",
            }
        ],
    }
    persist_pending_lessons(state)
    assert saved["profile"]["lessons"]["tables"]["accounts"] == ["champ x jamais NULL"]


def test_persist_writes_correction_lesson_on_convergence(monkeypatch):
    saved = _patch_profile_io(monkeypatch)
    _patch_verdict(monkeypatch, "Bon")
    state = {
        "session": "s1",
        "test_uid": "u1",
        "pending_lessons": [
            {
                "scope": "join",
                "key": "orders↔users",
                "rule": "user_id doit exister dans users",
                "source": "correction",
            }
        ],
    }
    persist_pending_lessons(state)
    assert saved["profile"]["lessons"]["joins"]["orders↔users"] == [
        "user_id doit exister dans users"
    ]
