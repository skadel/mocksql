import json

import sqlglot as sg
from sqlglot import MappingSchema
from sqlglot import expressions as exp
from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
from sqlglot.optimizer.qualify_columns import qualify_columns
from sqlglot.optimizer.qualify_tables import qualify_tables
from sqlglot.optimizer.scope import traverse_scope

from utils.examples import strip_qualifiers_with_scope


def extract_select_statement(sql: str, dialect: str) -> str | None:
    """
    Extrait le premier SELECT/WITH d'un script SQL pouvant contenir des DECLARE, SET, etc.
    Retourne None si aucun SELECT n'est trouvé.
    """
    parsed = _parse_select_ast(sql, dialect)
    if parsed is None:
        return None
    return parsed.sql(dialect=dialect, pretty=True)


def _parse_select_ast(sql: str, dialect: str) -> sg.exp.Expression | None:
    """Retourne l'AST du premier SELECT/WITH, en skippant les DECLARE/SET."""
    statements = sg.parse(sql, dialect=dialect)
    return next(
        (s for s in statements if isinstance(s, (sg.exp.Query, sg.exp.With))),
        None,
    )


def extract_real_table_refs(sql: str, dialect: str) -> list[sg.exp.Table]:
    """
    Extrait les références de tables réelles (physiques ou logiques) d'une requête SQL.

    Cette fonction analyse le SQL et parcourt ses différentes portées (scopes).
    Elle ignore volontairement :
    - Les Common Table Expressions (CTEs)
    - Les sous-requêtes
    - Les fonctions de table (ex: UNNEST, générateurs de tableaux, fonctions personnalisées)
    - Les artefacts syntaxiques (ex: références CTE masquées dans des nœuds PIVOT/UNPIVOT)

    IMPORTANT: Cette fonction contourne un bug connu de SQLglot où les CTEs référencées
    dans des clauses UNPIVOT ne sont pas correctement identifiées dans les scopes.

    Args:
        sql (str): La requête SQL à analyser.
        dialect (str): Le dialecte SQL ciblé (ex: "bigquery", "snowflake").

    Returns:
        list[sg.exp.Table]: Une liste d'objets Table représentant les dépendances réelles.

    Raises:
        sqlglot.ParseError: Si le SQL est syntaxiquement incorrect.
    """
    # Skips DECLARE/SET statements — finds the actual SELECT/WITH.
    parsed = _parse_select_ast(sql, dialect)
    if parsed is None:
        return []

    real_tables: list[sg.exp.Table] = []

    # 1. Collecte de TOUTES les CTEs définies dans la requête (nom → nœud CTE)
    # Cette approche globale contourne les bugs SQLglot où scope.ctes est vide
    # dans certains scopes (UNPIVOT/PIVOT, sous-requêtes dérivées imbriquées)
    # même quand des CTEs y sont référencées.
    all_ctes = _extract_all_ctes(parsed)

    # 2. Analyse des sources de données par scope avec filtrage robuste
    for scope in traverse_scope(parsed):
        active_cte_names = _extract_scope_cte_names(scope)

        for source in scope.sources.values():
            if not isinstance(source, sg.exp.Table):
                continue

            if not source.name:
                continue

            # Ignorer les fonctions de table (ex: UNNEST, my_function()).
            # Une table réelle possède un nœud parent 'this' de type Identifier.
            if not isinstance(source.this, sg.exp.Identifier):
                continue

            # Gestion des artefacts (PIVOT/UNPIVOT) et du masquage (shadowing)
            if _is_cte_reference(source, active_cte_names, all_ctes):
                continue

            if not _is_duplicate_table(source, real_tables):
                real_tables.append(source)

    return real_tables


def _extract_all_ctes(parsed: sg.exp.Expression) -> dict[str, sg.exp.CTE]:
    """
    Extrait toutes les CTEs définies dans la requête complète (nom minuscule → nœud).

    Workaround pour les bugs SQLglot où les CTEs ne sont pas propagées dans
    certains scopes (UNPIVOT/PIVOT, sous-requêtes dérivées imbriquées). Le nœud
    CTE est conservé pour distinguer une vraie référence de CTE d'un masquage
    (une vraie table partageant le nom d'une CTE à l'intérieur de sa définition).
    """
    all_ctes: dict[str, sg.exp.CTE] = {}

    for with_stmt in parsed.find_all(sg.exp.With):
        for cte in with_stmt.expressions:
            if isinstance(cte, sg.exp.CTE) and cte.alias:
                all_ctes[cte.alias.lower()] = cte

    return all_ctes


def _extract_scope_cte_names(scope) -> set[str]:
    """
    Extrait les noms des CTEs du scope courant avec gestion de rétrocompatibilité.
    """
    active_cte_names = set()

    if isinstance(scope.ctes, dict):
        active_cte_names.update(k.lower() for k in scope.ctes.keys())
    elif isinstance(scope.ctes, list):
        for item in scope.ctes:
            if isinstance(item, str):
                active_cte_names.add(item.lower())
            elif hasattr(item, "alias"):
                active_cte_names.add(item.alias.lower())

    return active_cte_names


def _is_cte_reference(
    source: sg.exp.Table,
    active_cte_names: set[str],
    all_ctes: dict[str, sg.exp.CTE],
) -> bool:
    """
    Détermine si une table est une référence à une CTE.

    Stratégie hybride :
    - Vérification normale via scope.ctes (cas standard).
    - Fallback global via all_ctes : SQLglot laisse scope.ctes vide dans
      certains scopes (PIVOT/UNPIVOT, sous-requêtes dérivées imbriquées) même
      quand une CTE top-level y est référencée. Une référence non qualifiée dont
      le nom correspond à une CTE est donc traitée comme une référence de CTE…
    - …SAUF si elle est lexicalement à l'intérieur de la définition de cette même
      CTE : là, le nom désigne une vraie table masquée (une CTE ne peut pas se
      référencer elle-même de façon non récursive). C'est le seul cas où une
      référence non qualifiée homonyme d'une CTE pointe vers une vraie table.
    """
    is_unqualified = not source.db and not source.catalog
    if not is_unqualified:
        return False

    source_name_lower = source.name.lower()

    if source_name_lower in active_cte_names:
        return True

    cte_node = all_ctes.get(source_name_lower)
    if cte_node is not None and not _is_within(source, cte_node):
        return True

    return False


def _is_within(node: sg.exp.Expression, ancestor: sg.exp.Expression) -> bool:
    """Vrai si ``node`` est un descendant de ``ancestor`` (ou est ``ancestor``)."""
    current: sg.exp.Expression | None = node
    while current is not None:
        if current is ancestor:
            return True
        current = current.parent
    return False


def _is_duplicate_table(
    source: sg.exp.Table, existing_tables: list[sg.exp.Table]
) -> bool:
    """
    Vérifie si la table est déjà présente dans la liste basée sur sa signature complète.
    """
    source_signature = _get_table_signature(source)

    for existing in existing_tables:
        if _get_table_signature(existing) == source_signature:
            return True

    return False


def _get_table_signature(table: sg.exp.Table) -> str:
    """
    Génère une signature unique pour une table basée sur catalog.schema.table.
    """
    catalog = table.catalog or ""
    schema = table.db or ""
    name = table.name or ""

    return f"{catalog}.{schema}.{name}"


def build_sql(sub_questions: list[dict] | None, final_sql: str) -> str:
    """
    Construit une requête SQL du type :

        WITH
          cte1 AS (...),
          cte2 AS (...),
          ...
        <final_sql>

    Si sub_questions est None ou vide, on retourne simplement final_sql.

    Parameters
    ----------
    sub_questions : list[dict] | None
        Chaque dict doit contenir au moins les clés
        - "sub_question_name" : nom de la CTE
        - "sub_query"         : texte SQL de la sous-requête
    final_sql : str
        Le SELECT final qui exploite les CTE précédentes.

    Returns
    -------
    str
        La requête SQL complète prête à exécuter.
    """
    # Si pas de sous-questions, on renvoie directement le SQL final
    if not sub_questions:
        return final_sql.strip()

    # Sinon, on formate chaque sous-requête comme une CTE
    ctes_parts = []
    for q in sub_questions:
        # Sécurité : s’assurer que q contient bien les deux clés attendues
        name = q.get("sub_question_name", "").strip()
        query_text = q.get("sub_query", "").strip()
        if not name or not query_text:
            # On peut soit lever une exception, soit sauter cette sous-question…
            # Ici on choisit de sauter si le format est incorrect
            continue

        ctes_parts.append(f"{name} AS (\n{query_text}\n)")

    # Si, après filtrage, il n’y a plus rien à mettre en CTE, on retourne le final_sql
    if not ctes_parts:
        return final_sql.strip()

    # On joint les CTE par virgule et on ajoute le SELECT final
    with_clause = ",\n".join(ctes_parts)
    return f"WITH\n{with_clause}\n{final_sql.strip()}"


def extract_used_columns(last_query_decomposed: list):
    """
    Extrait les colonnes utilisées pour chaque table avec le projet et la base à partir de la décomposition de la requête.
    Si plusieurs sources se réfèrent à la même table, leurs colonnes utilisées sont fusionnées (union).

    Args:
        last_query_decomposed (list): Liste de dictionnaires contenant les différentes étapes (queries)
                                      avec une clé 'sources' pour les sources de données. Chaque source
                                      doit contenir les clés 'project', 'database', 'table' et 'used_columns'.

    Returns:
        list: Liste de dictionnaires avec pour chaque table le projet, la base et la liste des colonnes utilisées.
    """
    used_columns_by_table = {}
    table_info = {}  # Pour stocker project et database par table

    # Parcourir chaque requête dans last_query_decomposed
    for query in last_query_decomposed:
        sources = query.get("sources", [])
        for source in sources:
            table = source.get("table")
            if not table:
                continue

            # Utiliser uniquement le nom de la table comme clé
            if table not in used_columns_by_table:
                used_columns_by_table[table] = set()
                # On initialise avec les valeurs rencontrées (si non vides)
                table_info[table] = {
                    "project": source.get("project", ""),
                    "database": source.get("database", ""),
                }
            # Mettre à jour l'ensemble des colonnes utilisées
            used_columns_by_table[table].update(source.get("used_columns", []))
            # Si les informations de project/database ne sont pas encore renseignées, on les met à jour
            if not table_info[table]["project"] and source.get("project"):
                table_info[table]["project"] = source.get("project")
            if not table_info[table]["database"] and source.get("database"):
                table_info[table]["database"] = source.get("database")

    # Construire la liste finale en fusionnant les colonnes et en conservant les infos projet et base
    used_columns_list = [
        {
            "project": table_info[table]["project"],
            "database": table_info[table]["database"],
            "table": table,
            "used_columns": sorted(list(used_columns_by_table[table])),
        }
        for table in used_columns_by_table
    ]

    return used_columns_list


def process_sql(sql_data, dialect, remove_db=False) -> str:
    if isinstance(sql_data, list):
        # Check if the only element in sql_data is 'final_query'
        if all(step["name"] == "final_query" for step in sql_data):
            return next(
                (step["code"] for step in sql_data if step["name"] == "final_query"), ""
            )

        # Construct the WITH clause by filtering out 'final_query'
        with_clause = "WITH " + ",\n".join(
            f"{step['name']} AS (\n{step['code']}\n)"
            for step in sql_data
            if step["name"] != "final_query"
        )

        # Add the code of 'final_query' separately
        final_query = next(
            (step["code"] for step in sql_data if step["name"] == "final_query"), ""
        )
        all_sql_code = f"{with_clause}\n{final_query}\n"
        if remove_db:
            used_columns_list = extract_used_columns(last_query_decomposed=sql_data)
            table_names = []
            for entry in used_columns_list:
                if entry["project"] != "":
                    table_names.append(f"{entry['project']}.{entry['table']}")
                else:
                    table_names.append(entry["table"])
            if len(table_names) == 0:
                raise ValueError("This script has no used columns.")
            all_sql_code = strip_qualifiers_with_scope(all_sql_code, dialect)
        # Combine the WITH clause and the final query
        return all_sql_code
    elif isinstance(sql_data, str):
        return sql_data
    else:
        raise ValueError("mocksql sql Expression should be list or str.")


def safe_process_sql(content, dialect, remove_db=False):
    try:
        data = json.loads(content)
        return process_sql(data, dialect, remove_db)
    except (json.JSONDecodeError, TypeError):
        # Si le content n'est pas un JSON valide, on renvoie simplement le contenu original
        return content


def get_all_columns_with_sources(
    sql_expression: sg.exp.Expression,
    schema_mapping: dict | None = None,
) -> list[dict]:
    """Retourne pour chaque table source les colonnes réellement référencées dans l'AST qualifié.

    ``schema_mapping`` (``{"dataset.table": {col: type, ...}}``, ou clé courte
    ``table``) sert à rattacher les colonnes **non qualifiées** : sur des CTEs
    complexes, ``qualify_columns`` laisse parfois une colonne de table de base
    sans préfixe (il n'ose pas deviner la table). Sans cette résolution, la
    colonne disparaît de ``used_columns`` → la table de test est créée sans elle
    → DuckDB "column not found". On l'attribue alors à toute table de base du
    scope dont le schéma déclare ce nom : la sur-inclusion est sûre (une colonne
    générée en trop ne casse jamais l'exécution), l'omission ne l'est pas.
    """
    col_with_sources: dict[tuple, dict] = {}

    # Index case-insensitive des colonnes connues par table, pour la résolution
    # des colonnes nues uniquement.
    schema_index: dict[str, set[str]] = {}
    if schema_mapping:
        for table_key, cols in schema_mapping.items():
            schema_index[table_key.lower()] = {str(c).lower() for c in cols}

    def _record(project_text, database_text, table_name_text, alias_text, col_name):
        key = (project_text, database_text, table_name_text)
        if key not in col_with_sources:
            col_with_sources[key] = {
                "project": project_text,
                "database": database_text,
                "table": table_name_text,
                "alias": alias_text,
                "used_columns": set(),
            }
        col_with_sources[key]["used_columns"].add(col_name.lower())

    def _base_tables(scope):
        """Tables de base (exp.Table) du scope, avec leurs colonnes de schéma."""
        out = []
        for alias_key, source in scope.sources.items():
            if not isinstance(source, exp.Table):
                continue
            table_token = source.this
            table_name_text = table_token.this if table_token else alias_key
            alias = source.args.get("alias")
            alias_text = alias.text("this") if alias else table_name_text
            project_text = source.catalog
            database_text = source.db
            cols: set[str] = set()
            if schema_index:
                qualified = (
                    f"{database_text}.{table_name_text}".lower()
                    if database_text
                    else table_name_text.lower()
                )
                cols = (
                    schema_index.get(qualified)
                    or schema_index.get(table_name_text.lower())
                    or set()
                )
            out.append((project_text, database_text, table_name_text, alias_text, cols))
        return out

    def _pivot_aliases(scope):
        """Map alias_pivot.lower() → (table de base, valeurs générées par le pivot).

        En BigQuery, ``PIVOT`` expose dans sa sortie les colonnes de base qui ne
        sont ni la valeur agrégée ni la colonne ``FOR`` (colonnes de regroupement
        implicites), plus une colonne par valeur de la liste ``IN``. Les premières
        proviennent de la table de base ; il faut donc rattacher toute référence
        ``alias_pivot.col`` (où ``col`` n'est pas une valeur générée) à cette table.
        """
        out: dict[str, tuple] = {}
        for source in scope.sources.values():
            if not isinstance(source, exp.Table):
                continue
            pivots = source.args.get("pivots") or []
            for pivot in pivots:
                if pivot.args.get("unpivot"):
                    continue
                alias = pivot.args.get("alias")
                alias_name = alias.text("this") if alias else None
                if not alias_name:
                    continue
                value_cols: set[str] = set()
                for field in pivot.args.get("fields") or []:
                    for value in field.find_all(exp.Alias, exp.Literal):
                        name = (
                            value.alias if isinstance(value, exp.Alias) else value.name
                        )
                        if name:
                            value_cols.add(name.lower())
                table_token = source.this
                table_name_text = table_token.this if table_token else alias_name
                tbl_alias = source.args.get("alias")
                alias_text = tbl_alias.text("this") if tbl_alias else table_name_text
                out[alias_name.lower()] = (
                    source.catalog,
                    source.db,
                    table_name_text,
                    alias_text,
                    value_cols,
                )
        return out

    def _source_by_qualifier(scope):
        """Map case-insensitive {alias, table, db.table} → (project, db, table, alias)
        des tables de base du scope.

        Permet de rattacher une colonne dont le qualificateur est le **nom** de la table
        (ou ``db.table``) plutôt que son alias assigné. Cas produit par la restauration de
        casse d'``optimize_query`` sur une table référencée par son nom complet backtické
        sans alias (``\\`DS.TABLE\\`.col``) : qualify aliase la table (``AS TABLE``) mais
        laisse le qualificateur de colonne sous le nom complet (``ds.table``), qui ne
        matche alors plus aucune clé de ``scope.sources`` (l'alias). Sans ce repli, la
        colonne tombe dans la « table fantôme » et disparaît de used_columns (incident c3).
        """
        out: dict[str, tuple] = {}
        for alias_key, source in scope.sources.items():
            if not isinstance(source, exp.Table):
                continue
            table_token = source.this
            table_name_text = table_token.this if table_token else alias_key
            alias = source.args.get("alias")
            alias_text = alias.text("this") if alias else table_name_text
            ident = (source.catalog, source.db, table_name_text, alias_text)
            keys = {alias_key.lower(), (alias_text or "").lower(), table_name_text.lower()}
            if source.db:
                keys.add(f"{source.db}.{table_name_text}".lower())
            for k in keys:
                if k:
                    out.setdefault(k, ident)
        return out

    def _extract(scope) -> None:
        base_tables = None  # résolu paresseusement, seulement si besoin
        pivot_aliases = None  # résolu paresseusement, seulement si besoin
        source_resolver = None  # résolu paresseusement, seulement si besoin
        for column in scope.columns:
            table_alias = column.table

            if table_alias in scope.sources:
                source = scope.sources[table_alias]
                if isinstance(source, exp.Unnest):
                    continue
                if isinstance(source, exp.Table):
                    table_token = source.this
                    table_name_text = table_token.this if table_token else table_alias
                    alias = source.args.get("alias")
                    alias_text = alias.text("this") if alias else table_name_text
                    project_text = source.catalog
                    database_text = source.db
                else:
                    table_name_text = table_alias
                    alias_text = table_alias
                    project_text = None
                    database_text = None
                if not table_name_text:
                    continue
                _record(
                    project_text,
                    database_text,
                    table_name_text,
                    alias_text,
                    column.name,
                )
            elif not table_alias and schema_index:
                # Colonne nue : qualify_columns n'a pas su la rattacher. On
                # l'attribue à chaque table de base du scope qui la déclare.
                if base_tables is None:
                    base_tables = _base_tables(scope)
                cname = column.name.lower()
                for (
                    project_text,
                    database_text,
                    table_name_text,
                    alias_text,
                    cols,
                ) in base_tables:
                    if cname in cols:
                        _record(
                            project_text,
                            database_text,
                            table_name_text,
                            alias_text,
                            column.name,
                        )
            else:
                # table_alias présent mais hors sources. Cas PIVOT : l'alias
                # virtuel du pivot porte les colonnes de regroupement implicites,
                # qui proviennent de la table de base.
                if not table_alias:
                    continue
                if pivot_aliases is None:
                    pivot_aliases = _pivot_aliases(scope)
                pivot = pivot_aliases.get(table_alias.lower())
                if pivot is not None:
                    (
                        project_text,
                        database_text,
                        table_name_text,
                        alias_text,
                        value_cols,
                    ) = pivot
                    if column.name.lower() not in value_cols:
                        _record(
                            project_text,
                            database_text,
                            table_name_text,
                            alias_text,
                            column.name,
                        )
                        continue
                    # Colonne générée par le pivot : pas une colonne de base.
                    continue
                # Repli avant la « table fantôme » : le qualificateur peut être le NOM de
                # la table (ou ``db.table``) plutôt que son alias — on le rattache à la
                # table de base correspondante (case-insensitive) pour ne pas perdre la
                # colonne. cf. _source_by_qualifier (incident c3).
                if source_resolver is None:
                    source_resolver = _source_by_qualifier(scope)
                candidates = [table_alias.lower()]
                if column.db:
                    candidates.insert(0, f"{column.db}.{table_alias}".lower())
                resolved = next(
                    (source_resolver[c] for c in candidates if c in source_resolver),
                    None,
                )
                if resolved is not None:
                    project_text, database_text, table_name_text, alias_text = resolved
                    _record(
                        project_text,
                        database_text,
                        table_name_text,
                        alias_text,
                        column.name,
                    )
                    continue
                _record(None, None, table_alias, table_alias, column.name)

    global_columns: set[str] = set()
    for col in sql_expression.find_all(exp.Column):
        if hasattr(col, "parts"):
            for part in col.parts:
                global_columns.add(part.name.lower())
        else:
            global_columns.add(col.name.lower())

    for scope in traverse_scope(sql_expression):
        _extract(scope)

    result = []
    for _, info in sorted(
        col_with_sources.items(), key=lambda x: (x[0][0] or "", x[0][1] or "", x[0][2])
    ):
        info["used_columns"] = list(info["used_columns"])
        info["used_identifiers"] = list(global_columns)
        result.append(info)
    return result


def extract_used_columns_from_sql(
    sql: str, dialect: str, schemas: list[dict]
) -> list[str]:
    """Extrait les colonnes réellement référencées dans sql et retourne la liste JSON-encodée.

    Utilise sqlglot qualify_tables + qualify_columns pour résoudre les alias, puis
    get_all_columns_with_sources pour ne conserver que les colonnes des vraies tables
    (celles avec un database, donc pas les CTEs).
    """
    mapping: dict[str, dict] = {}
    for tbl in schemas:
        parts = tbl["table_name"].split(".")
        key = ".".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
        mapping[key] = {
            col["name"]: col.get("bq_ddl_type") or col.get("type", "STRING")
            for col in tbl.get("columns", [])
            if "." not in col["name"]
        }

    schema = MappingSchema()
    for table_name, cols in mapping.items():
        schema.add_table(table_name, cols, dialect=dialect)

    parsed = sg.parse_one(sql, read=dialect)
    parsed = normalize_identifiers(parsed, dialect=dialect)
    parsed = qualify_tables(parsed)
    parsed = qualify_columns(parsed, schema, infer_schema=True)

    result = []
    for entry in get_all_columns_with_sources(parsed, schema_mapping=mapping):
        if not entry.get("database"):
            continue  # CTE ou ref non résolue
        result.append(
            json.dumps(
                {
                    "project": entry["project"] or "",
                    "database": entry["database"],
                    "table": entry["table"],
                    "used_columns": sorted(entry["used_columns"]),
                }
            )
        )
    return result
