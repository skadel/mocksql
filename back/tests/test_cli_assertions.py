"""Régression : helpers purs de manipulation d'assertions pour la CLI `assert`.

Ces helpers sous-tendent `mocksql assert list/add/update/remove`, qui permettent
à un agent de code de poser une spec (assertion cible rouge) puis d'itérer le SQL
source jusqu'au vert. Le ciblage se fait par `test_uid` (test) + `assertion_uid`
(assertion) — jamais par position, cf. l'invariant d'identité.
"""

import pytest

from cli.assertions import (
    add_assertion,
    ensure_assertion_uids,
    find_test_case,
    remove_assertion,
    update_assertion,
)


def _doc():
    return {
        "test_cases": [
            {
                "test_uid": "tc-aaa",
                "assertion_results": [
                    {
                        "description": "déjà là",
                        "sql": "SELECT * FROM __result__ WHERE x != 1",
                    },
                ],
            },
            {"test_uid": "tc-bbb", "assertion_results": []},
        ]
    }


def test_find_test_case_by_uid():
    doc = _doc()
    tc = find_test_case(doc, "tc-bbb")
    assert tc is not None and tc["test_uid"] == "tc-bbb"


def test_find_test_case_unknown_returns_none():
    assert find_test_case(_doc(), "tc-zzz") is None


def test_add_assertion_gets_short_uid_and_appends():
    tc = {"test_uid": "tc-bbb", "assertion_results": []}
    a = add_assertion(
        tc, "le total carte vaut 150", "SELECT * FROM __result__ WHERE total != 150"
    )
    assert a["assertion_uid"] and len(a["assertion_uid"]) == 8
    assert tc["assertion_results"] == [a]
    assert a["description"] == "le total carte vaut 150"


def test_add_assertion_creates_assertion_results_if_missing():
    tc = {"test_uid": "tc-bbb"}
    add_assertion(tc, "d", "SELECT * FROM __result__ WHERE 1=0")
    assert len(tc["assertion_results"]) == 1


def test_ensure_assertion_uids_backfills_existing():
    tc = _doc()["test_cases"][0]
    ensure_assertion_uids(tc)
    uid = tc["assertion_results"][0]["assertion_uid"]
    assert uid and len(uid) == 8
    # idempotent : un second passage ne change pas l'uid
    ensure_assertion_uids(tc)
    assert tc["assertion_results"][0]["assertion_uid"] == uid


def test_update_assertion_by_uid():
    tc = {"test_uid": "x", "assertion_results": []}
    a = add_assertion(tc, "old", "SELECT * FROM __result__ WHERE 1=0")
    update_assertion(
        tc,
        a["assertion_uid"],
        description="new",
        sql="SELECT * FROM __result__ WHERE y != 2",
    )
    assert a["description"] == "new"
    assert a["sql"] == "SELECT * FROM __result__ WHERE y != 2"


def test_update_assertion_unknown_uid_raises():
    tc = {"test_uid": "x", "assertion_results": []}
    with pytest.raises(KeyError):
        update_assertion(tc, "nope", description="x")


def test_remove_assertion_by_uid():
    tc = {"test_uid": "x", "assertion_results": []}
    a = add_assertion(tc, "d", "SELECT * FROM __result__ WHERE 1=0")
    removed = remove_assertion(tc, a["assertion_uid"])
    assert removed["assertion_uid"] == a["assertion_uid"]
    assert tc["assertion_results"] == []


def test_remove_assertion_unknown_uid_raises():
    tc = {"test_uid": "x", "assertion_results": []}
    with pytest.raises(KeyError):
        remove_assertion(tc, "nope")
