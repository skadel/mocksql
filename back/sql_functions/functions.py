from typing import List, Tuple

from sqlglot import parse_one

from sql_functions.helpers import (
    collect_needed_by_table,
    propagate_on_tree,
    cte_info_map,
    propagate_cte_aliases_to_consumers,
)


# -----------------------
# API principale
# -----------------------
def add_detail_columns(
    sql: str,
    dialect_in: str,
    dialect_out: str,
    to_add: List[Tuple[str, str]],
) -> str:
    """
    Ajoute des colonnes de détail :
    - __detail_<alias_table>_<col> quand la table physique est visible,
    - si agrégats : ajoute aussi au GROUP BY,
    - aligne les UNION,
    - propage via CTE des alias nus (évite u.* hors portée),
    - n'injecte jamais dans un SELECT qui contient un PIVOT
      et ne propage pas depuis une CTE pivotée.
    """
    root = parse_one(sql, read=dialect_in)
    need = collect_needed_by_table(to_add)

    # 1) Injection dans tous les SELECT/UNION
    propagate_on_tree(root, need)

    # 2) Propagation CTE -> consommateurs (alias nus), en évitant PIVOT
    ctes = cte_info_map(root)
    propagate_cte_aliases_to_consumers(root, ctes)

    return root.sql(dialect=dialect_out)
