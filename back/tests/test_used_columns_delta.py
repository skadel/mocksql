"""Tests du moteur de diff de schéma (régénération partielle sur changement de source)."""

import json

from build_query.validator import (
    compute_used_columns_delta,
    is_delta_patchable,
)


def _uc(table, columns, project="p", database="d"):
    return {
        "project": project,
        "database": database,
        "table": table,
        "used_columns": columns,
    }


def test_table_swap_with_overlap_is_patchable():
    """Remplacer une source par une autre avec colonnes recouvrantes : la table change
    mais le delta reste patchable (la nouvelle table partage des colonnes existantes)."""
    old = [_uc("silver", ["id", "amount", "ts"]), _uc("dim", ["id", "label"])]
    new = [_uc("gold", ["id", "amount", "ts"]), _uc("dim", ["id", "label"])]

    delta = compute_used_columns_delta(old, new)

    added = {e["table"] for e in delta["tables_added"]}
    removed = {e["table"] for e in delta["tables_removed"]}
    assert added == {"p.d.gold"}
    assert removed == {"p.d.silver"}
    assert delta["columns_added"] == []
    assert delta["columns_removed"] == []
    assert is_delta_patchable(delta, new) is True


def test_added_and_removed_columns_same_table():
    old = [_uc("orders", ["id", "total", "old_flag"])]
    new = [_uc("orders", ["id", "total", "new_flag"])]

    delta = compute_used_columns_delta(old, new)

    assert delta["tables_added"] == []
    assert delta["tables_removed"] == []
    assert delta["columns_added"] == [{"table": "p.d.orders", "columns": ["new_flag"]}]
    assert delta["columns_removed"] == [
        {"table": "p.d.orders", "columns": ["old_flag"]}
    ]
    assert is_delta_patchable(delta, new) is True


def test_brand_new_table_without_overlap_is_not_patchable():
    """Une table neuve sans aucune colonne commune → patch incrémental impossible :
    bascule attendue vers la régénération complète."""
    old = [_uc("orders", ["id", "total"])]
    new = [
        _uc("orders", ["id", "total"]),
        _uc("gold_events", ["event_uuid", "payload", "kind"]),
    ]

    delta = compute_used_columns_delta(old, new)

    assert {e["table"] for e in delta["tables_added"]} == {"p.d.gold_events"}
    assert is_delta_patchable(delta, new) is False


def test_accepts_json_string_entries():
    """Le format stocké est une liste de strings JSON (cf. validator) — accepté aussi."""
    old = [json.dumps(_uc("silver", ["id", "amount"]))]
    new = [json.dumps(_uc("gold", ["id", "amount"]))]

    delta = compute_used_columns_delta(old, new)

    assert {e["table"] for e in delta["tables_added"]} == {"p.d.gold"}
    assert {e["table"] for e in delta["tables_removed"]} == {"p.d.silver"}


def test_no_change_yields_empty_delta():
    cols = [_uc("orders", ["id", "total"])]
    delta = compute_used_columns_delta(cols, cols)
    assert not any(delta.values())
