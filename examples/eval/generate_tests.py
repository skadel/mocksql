"""
Génère les tests MockSQL pour une liste de modèles via l'API HTTP (stream SSE).
Usage :
  python generate_tests.py --models bq282 bq199
  python generate_tests.py --models bq282 bq199 --url http://localhost:8100 --project pipetalk-493612
"""

import argparse
import json
import re
import sys
import uuid
import time

import requests


def _get_sql(base_url: str, model_name: str) -> str:
    r = requests.get(f"{base_url}/api/models/sql", params={"name": model_name}, timeout=10)
    r.raise_for_status()
    return r.json()["sql"]


def _create_session(base_url: str, model_name: str) -> str:
    """Crée un test si absent, sinon retourne le session_id existant."""
    # Cherche d'abord une session existante
    r = requests.get(f"{base_url}/api/models", timeout=10)
    r.raise_for_status()
    for m in r.json():
        if m.get("name") == model_name and m.get("session_id"):
            print(f"  session existante : {m['session_id']}")
            return m["session_id"]

    r = requests.post(
        f"{base_url}/api/tests",
        json={"model_name": model_name},
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["test_id"]


def _validate_sql(base_url: str, sql: str, session_id: str, project: str) -> dict:
    """Appelle /validate-query pour extraire used_columns et optimized_sql."""
    r = requests.post(
        f"{base_url}/api/validate-query",
        json={"sql": sql, "project": project, "dialect": "bigquery", "session": session_id},
        timeout=60,
    )
    r.raise_for_status()
    result = r.json()
    if not result.get("valid"):
        raise RuntimeError(f"Validation echouee: {result.get('error') or result}")
    return result


def _stream_generate(base_url: str, sql: str, session_id: str, project: str, used_columns: list) -> None:
    """Appelle le stream SSE et attend la fin (bloquant)."""
    body = {
        "input": {
            "input": "Génère des tests pour cette requête SQL.",
            "query": sql,
            "validated_sql": "",
            "optimized_sql": "",
            "user_tables": "",
            "profile_result": "",
            "dialect": "bigquery",
            "schemas": [],
            "session": session_id,
            "project": project,
            "user": "",
            "user_message_id": str(uuid.uuid4()),
            "parent_message_id": "",
            "changed_message_id": "",
            "request_id": str(uuid.uuid4()),
            "gen_retries": 2,
            "used_columns": [],
            "used_columns_changed": False,
            "optimize": True,
            "route": "",
            "status": "",
            "save": "",
            "title": "",
            "reasoning": "",
            "error": "",
            "current_query": "",
            "query_decomposed": "",
            "test_index": None,
            "rerun_all_tests": False,
            "assertion_only": False,
            "profile_complete": None,
            "profile": None,
            "profile_billing_tb": None,
            "messages": [],
            "history": [],
            "examples": [],
        },
        "config": {},
    }

    with requests.post(
        f"{base_url}/api/query/build/stream_events",
        json=body,
        stream=True,
        timeout=300,
    ) as resp:
        resp.raise_for_status()
        for raw_line in resp.iter_lines():
            if not raw_line:
                continue
            line = raw_line.decode("utf-8") if isinstance(raw_line, bytes) else raw_line
            if not line.startswith("data: "):
                continue
            try:
                event = json.loads(line[6:])
            except json.JSONDecodeError:
                continue

            name = event.get("name", "")
            event_type = event.get("event", "")

            if event_type == "on_chain_start":
                print(f"    > {name}", flush=True)
            elif event_type == "on_chain_end" and name in ("generator", "executor", "test_evaluator"):
                print(f"    done: {name}", flush=True)
            elif event_type == "on_chain_end" and name == "history_saver":
                print(f"    done: history_saver - fin du graph", flush=True)
                break


def run(base_url: str, models: list[str], project: str) -> None:
    for model in models:
        print(f"\n[{model}] génération des tests …")
        try:
            sql = _get_sql(base_url, model)
            print(f"  SQL chargé ({len(sql)} chars)")
            session_id = _create_session(base_url, model)
            print(f"  session : {session_id}")
            _stream_generate(base_url, sql, session_id, project)
            print(f"  [ok] tests sauvegardés")
        except Exception as exc:
            print(f"  [erreur] {exc}", file=sys.stderr)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--models", nargs="+", required=True)
    parser.add_argument("--url", default="http://localhost:8100")
    parser.add_argument("--project", default="pipetalk-493612")
    args = parser.parse_args()
    run(args.url, args.models, args.project)


if __name__ == "__main__":
    main()
