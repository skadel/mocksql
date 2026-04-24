from typing import List, Tuple, Dict, Set

from sqlglot import exp
from sqlglot.optimizer.scope import traverse_scope

DETAIL_PREFIX = "__detail"


# -----------------------
# Helpers utilitaires
# -----------------------
def safe_arg(node: exp.Expression, key: str):
    d = getattr(node, "args", None)
    if isinstance(d, dict):
        return d.get(key)
    return None


def make_col(table_or_alias: str, col: str) -> exp.Column:
    return exp.column(col, table=table_or_alias)


def make_alias(expr: exp.Expression, alias: str) -> exp.Alias:
    return exp.alias_(expr, alias=alias)


def add_proj(select: exp.Select, expr: exp.Expression):
    select.set("expressions", (select.expressions or []) + [expr])


def expr_aliases(select: exp.Select) -> List[str]:
    out = []
    for e in select.expressions or []:
        a = e.alias_or_name
        if a:
            out.append(a)
    return out


def columns_present(select: exp.Select) -> Set[str]:
    out = set()
    for e in select.expressions or []:
        a = e.alias_or_name
        if a:
            out.add(a.lower())
        else:
            out.add(e.sql().lower())
    return out


def contains_pivot(select: exp.Select) -> bool:
    return select.find(exp.Pivot) is not None


# -----------------------
# Agrégats / GROUP BY
# -----------------------
def has_aggregates(select: exp.Select) -> bool:
    if safe_arg(select, "group"):
        return True
    for expression in select.expressions or []:
        if expression.find(exp.AggFunc):
            return True
    for key in ("having", "qualify"):
        node = safe_arg(select, key)
        if node and node.find(exp.AggFunc):
            return True
    return False


def ensure_in_group_by(select: exp.Select, col_exprs: List[exp.Expression]):
    group = safe_arg(select, "group")
    if not group:
        if has_aggregates(select):
            group = exp.Group(expressions=[])
            select.set("group", group)
        else:
            return
    existing = [g.sql() for g in (group.expressions or [])]
    for c in col_exprs:
        if c.sql() not in existing:
            group.set("expressions", (group.expressions or []) + [c.copy()])


# -----------------------
# Résolution des sources
# -----------------------
def alias_to_table(scope) -> Dict[str, str]:
    """
    Mappe alias visibles -> nom de table physique (minuscules).
    N'inclut PAS les CTE ni les sous-requêtes : on propage séparément.
    """
    out = {}
    for name, source in scope.sources.items():
        if isinstance(source, exp.Table):
            out[name.lower()] = source.name.lower()
    return out


def collect_needed_by_table(pairs: List[Tuple[str, str]]) -> Dict[str, Set[str]]:
    need: Dict[str, Set[str]] = {}
    for t, c in pairs:
        need.setdefault(t.lower(), set()).add(c.lower())
    return need


# -----------------------
# Injection dans SELECT
# -----------------------
def add_detail_columns_to_select(select: exp.Select, need: Dict[str, Set[str]]):
    """
    Ajoute __detail_<alias>_<col> dans le SELECT si la table physique est visible.
    Si agrégats : ajoute aussi au GROUP BY.
    Ne modifie pas un SELECT qui contient un PIVOT.
    """
    if contains_pivot(select):
        return

    scopes = list(traverse_scope(select))
    if not scopes:
        return
    scope = scopes[0]
    alias_map = alias_to_table(scope)  # seulement tables physiques

    table_to_alias: Dict[str, str] = {}
    for alias, tbl in alias_map.items():
        table_to_alias.setdefault(tbl, alias)

    already = columns_present(select)
    add_exprs: List[exp.Expression] = []
    group_candidates: List[exp.Expression] = []

    for tbl, cols in need.items():
        ref = table_to_alias.get(tbl)
        if not ref:
            continue
        for col in cols:
            alias_name = f"{DETAIL_PREFIX}_{ref}_{col}"
            if alias_name.lower() in already:
                continue
            ce = make_col(ref, col)
            add_exprs.append(make_alias(ce, alias_name))
            group_candidates.append(ce)

    for e in add_exprs:
        add_proj(select, e)
    if add_exprs and has_aggregates(select):
        ensure_in_group_by(select, group_candidates)


# -----------------------
# Gestion des UNION(s)
# -----------------------
def add_detail_columns_in_union(union: exp.Union, need: Dict[str, Set[str]]):
    branches = []

    def collect_leaves(expr):
        if isinstance(expr, exp.Union):
            collect_leaves(expr.left)
            collect_leaves(expr.right)
        else:
            branches.append(expr)

    collect_leaves(union)

    # 1) Ajout dans chaque branche
    for b in branches:
        sel = b if isinstance(b, exp.Select) else b.find(exp.Select)
        if isinstance(sel, exp.Select):
            add_detail_columns_to_select(sel, need)

    # 2) Superset d'alias dans l'ordre d'apparition
    ordered = []
    seen = set()
    for b in branches:
        sel = b if isinstance(b, exp.Select) else b.find(exp.Select)
        if isinstance(sel, exp.Select):
            for a in expr_aliases(sel):
                if a and a.lower() not in seen:
                    seen.add(a.lower())
                    ordered.append(a)

    # 3) Compléter avec NULL pour les manquants
    for b in branches:
        sel = b if isinstance(b, exp.Select) else b.find(exp.Select)
        if not isinstance(sel, exp.Select):
            continue
        present = {
            e.alias_or_name.lower(): True
            for e in (sel.expressions or [])
            if e.alias_or_name
        }
        for alias in ordered:
            if alias and alias.lower() not in present:
                add_proj(sel, make_alias(exp.Null(), alias))


# -----------------------
# Parcours récursif
# -----------------------
def propagate_on_tree(expr: exp.Expression, need: Dict[str, Set[str]]):
    if isinstance(expr, exp.Union):
        add_detail_columns_in_union(expr, need)

    if isinstance(expr, exp.Select):
        add_detail_columns_to_select(expr, need)

    for child in expr.args.values():
        if isinstance(child, list):
            for c in child:
                if isinstance(c, exp.Expression):
                    propagate_on_tree(c, need)
        elif isinstance(child, exp.Expression):
            propagate_on_tree(child, need)


# -----------------------
# CTE : collecte & propagation
# -----------------------
def cte_info_map(root: exp.Expression) -> Dict[str, Dict]:
    """
    { cte_name -> { 'aliases': [str], 'has_pivot': bool } }
    """
    out = {}
    for w in root.find_all(exp.With):
        for cte in w.find_all(exp.CTE):
            name = (cte.alias or "").lower()
            sel = cte.this.find(exp.Select)
            if name and isinstance(sel, exp.Select):
                out[name] = {
                    "aliases": [
                        e.alias_or_name
                        for e in (sel.expressions or [])
                        if e.alias_or_name
                    ],
                    "has_pivot": bool(sel.find(exp.Pivot)),
                }
    return out


def add_unqualified_aliases(select: exp.Select, alias_names: List[str]):
    if contains_pivot(select):
        return  # ne rien injecter dans un SELECT qui contient un PIVOT
    present = {
        e.alias_or_name.lower(): True
        for e in (select.expressions or [])
        if e.alias_or_name
    }
    to_add = [a for a in alias_names if a and a.lower() not in present]
    if not to_add:
        return
    for a in to_add:
        add_proj(select, exp.to_identifier(a))  # identifiant nu
    if has_aggregates(select):
        ensure_in_group_by(select, [exp.to_identifier(a) for a in to_add])


def propagate_cte_aliases_to_consumers(root: exp.Expression, ctes: Dict[str, Dict]):
    if not ctes:
        return
    for sel in root.find_all(exp.Select):
        scopes = list(traverse_scope(sel))
        if not scopes:
            continue
        scope = scopes[0]
        needed = []
        for src in scope.sources.keys():
            info = ctes.get(src.lower())
            if not info:
                continue
            if info.get("has_pivot"):
                # ne pas injecter depuis une CTE pivotée
                continue
            needed.extend(info.get("aliases", []))
        if needed:
            add_unqualified_aliases(sel, needed)
