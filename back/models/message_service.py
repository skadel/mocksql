import json
from typing import List, Dict, Any, Union, Optional

from langchain_core.messages import messages_from_dict, BaseMessage

from common_vars import COMMON_HISTORY_TABLE_NAME
from models.database import execute, query
from models.env_variables import DB_MODE


def parse_data(raw):
    if raw is None:
        return {}
    if isinstance(raw, dict):
        return raw
    if isinstance(raw, (bytes, bytearray)):
        try:
            return json.loads(raw.decode())
        except Exception:
            return {}
    if isinstance(raw, str):
        try:
            return json.loads(raw)
        except Exception:
            return {}
    return {}


def clean_parent_id(raw_parent):
    # Nettoie parent_id externe si on doit l’utiliser en fallback
    if raw_parent in (None, "", "null", '""'):
        return None
    if (
        isinstance(raw_parent, str)
        and raw_parent.startswith('"')
        and raw_parent.endswith('"')
    ):
        return raw_parent[1:-1]
    return raw_parent


async def get_messages_history(
    session_id: str, message_data_id: str
) -> List[BaseMessage]:
    """
    Récupère tous les messages associés à un `session_id` donné et reliés au message spécifié
    par `message_data_id`, en suivant la chaîne de parentage jusqu'au message racine.
    """
    # Étape 1 : trouver l'identifiant incrémental
    if DB_MODE in ("cloudsql", "postgres"):
        find_id_sql = f"""
        SELECT id
        FROM {COMMON_HISTORY_TABLE_NAME}
        WHERE session_id = $1
          AND data->>'id' = $2
        """
        rows = await query(find_id_sql, (session_id, message_data_id))

    elif DB_MODE == "duckdb":
        find_id_sql = f"""
        SELECT id
        FROM {COMMON_HISTORY_TABLE_NAME}
        WHERE session_id = ?
          AND json_extract_path_text(data, 'id') = ?
        """
        rows = await query(find_id_sql, (session_id, message_data_id))

    else:
        raise RuntimeError(f"Unsupported DB_MODE: {DB_MODE}")

    if not rows:
        return []
    incremental_id = rows[0]["id"]

    # Étape 2 : récupérer tous les messages jusqu'à cet ID
    if DB_MODE in ("cloudsql", "postgres"):
        all_msgs_sql = f"""
        SELECT
          id,
          data,
          type,
          data->'additional_kwargs'->>'parent' AS parent_id
        FROM {COMMON_HISTORY_TABLE_NAME}
        WHERE session_id = $1
          AND id <= $2
        ORDER BY id ASC
        """
        params = (session_id, incremental_id)

    else:  # duckdb
        all_msgs_sql = f"""
        SELECT
          id,
          data,
          type,
          CAST(
            json_extract(data, '$.additional_kwargs.parent')
            AS VARCHAR
          ) AS parent_id
        FROM {COMMON_HISTORY_TABLE_NAME}
        WHERE session_id = ?
          AND id <= ?
        ORDER BY id ASC
        """
        params = (session_id, incremental_id)

    records = await query(all_msgs_sql, params)
    messages_dict: Dict[str, Dict[str, Any]] = {}
    for rec in records:
        data = parse_data(rec.get("data"))
        msg_id = data.get("id")
        if not msg_id:  # évite d’écraser la clé None
            continue

        # 1) On préfère le parent interne (fiable)
        parent_from_data = (data.get("additional_kwargs") or {}).get("parent")
        # 2) Fallback : parent externe (nettoyé)
        parent_from_record = clean_parent_id(rec.get("parent_id"))
        parent_id = parent_from_data or parent_from_record

        messages_dict[msg_id] = {
            "data": data,
            "type": rec.get("type"),
            "parent_id": parent_id,
            "kind": (data.get("additional_kwargs") or {}).get("type"),
        }

    def build_chain(start_id: str) -> List[str]:
        chain, seen = [], set()
        cur = start_id
        while cur and cur not in seen:
            seen.add(cur)
            chain.append(cur)
            cur = messages_dict.get(cur, {}).get("parent_id")
        return list(reversed(chain))  # du plus ancien au plus récent

    ordered_ids = build_chain(message_data_id)

    filtered = [
        {
            "id": mid,
            "type": messages_dict[mid]["type"],
            "kind": messages_dict[mid]["kind"],
            "data": messages_dict[mid]["data"],
        }
        for mid in ordered_ids
        if mid in messages_dict
    ]

    return messages_from_dict(filtered)


async def delete_messages_from_index(
    session_id: str, index: int
) -> Dict[str, Union[bool, str]]:
    """
    Supprime tous les messages d'un certain index (ROW_NUMBER) pour une session.
    """
    try:
        sub_sql = f"""
        SELECT id FROM (
            SELECT id, ROW_NUMBER() OVER (ORDER BY id) AS row_num
            FROM {COMMON_HISTORY_TABLE_NAME}
            WHERE session_id = '{session_id}'
              AND data->'additional_kwargs'->>'type' NOT IN ('explanation_request','explanation')
        ) sub
        WHERE sub.row_num = {index + 1}
        """
        res = await query(sub_sql)
        if not res:
            return {"success": False, "error": "No ID found at the specified index"}
        cutoff_id = res[0]["id"]

        del_sql = f"""
        DELETE FROM {COMMON_HISTORY_TABLE_NAME}
        WHERE session_id = '{session_id}' AND id >= {cutoff_id}
        """
        await execute(del_sql)
        return {"success": True}
    except Exception as e:
        return {"success": False, "error": str(e)}


async def get_message_by_session_and_data_id(
    session_id: str, data_id: str
) -> Optional[Dict[str, Any]]:
    """
    Récupère un message par session et data->>'id'.
    """
    sql = f"""
    SELECT *
    FROM {COMMON_HISTORY_TABLE_NAME}
    WHERE session_id = $1 AND data->>'id' = $2
    """
    rows = await query(sql, (session_id, data_id))
    return dict(rows[0]) if rows else None


async def get_first_sql_query_after_parent(
    session_id: str, parent_message_id: str
) -> Optional[BaseMessage]:
    """
    Récupère le premier message SQL après un parent donné.
    """
    # ID du parent
    parent_sql = f"""
    SELECT id FROM {COMMON_HISTORY_TABLE_NAME}
    WHERE session_id = $1 AND data->>'id' = $2
    """
    parent_rows = await query(parent_sql, (session_id, parent_message_id))
    if not parent_rows:
        return None
    parent_db_id = parent_rows[0]["id"]

    # Premier SQL après
    sql = f"""
    SELECT data
    FROM {COMMON_HISTORY_TABLE_NAME}
    WHERE session_id = $1
      AND id > $2
      AND data->'additional_kwargs'->>'type' = 'sql'
    ORDER BY id ASC
    LIMIT 1
    """
    rows = await query(sql, (session_id, parent_db_id))
    if not rows:
        return None
    data = rows[0]["data"]
    if not isinstance(data, dict):
        try:
            data = json.loads(data)
        except Exception:
            data = {}
    [msg] = messages_from_dict([{"data": data, "type": "sql"}])
    return msg


async def get_messages_after_data_id(
    session_id: str, data_id: str
) -> List[BaseMessage]:
    """
    Récupère tous les messages dont data->>'id' est supérieur à data_id.
    """
    sql = f"""
    SELECT data, type
    FROM {COMMON_HISTORY_TABLE_NAME}
    WHERE session_id = $1 AND data->>'id' > $2
    ORDER BY id ASC
    """
    rows = await query(sql, (session_id, data_id))
    items = []
    for r in rows:
        d = r["data"]
        if not isinstance(d, dict):
            try:
                d = json.loads(d)
            except Exception:
                d = {}
        items.append({"data": d, "type": r["type"]})
    return messages_from_dict(items)


async def update_message_by_data_id(
    session_id: str, message_data_id: str, updated_data: Dict[str, Any]
) -> Optional[BaseMessage]:
    """
    Met à jour le JSON d'un message par data->>'id' et renvoie le BaseMessage.
    """
    sql = f"""
    UPDATE {COMMON_HISTORY_TABLE_NAME}
    SET data = $3::jsonb
    WHERE session_id = $1 AND data->>'id' = $2
    RETURNING data, type
    """
    rows = await query(sql, (session_id, message_data_id, json.dumps(updated_data)))
    if not rows:
        return None
    row = rows[0]
    data = row["data"]
    if not isinstance(data, dict):
        try:
            data = json.loads(data)
        except Exception:
            data = {}
    [msg] = messages_from_dict([{"data": data, "type": row["type"]}])
    return msg
