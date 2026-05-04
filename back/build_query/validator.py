import json
import re
from collections import defaultdict

import sqlglot
from google.cloud import bigquery
from langchain_core.messages import AIMessage
from sqlglot import MappingSchema
from sqlglot import expressions as exp
from sqlglot.optimizer.scope import traverse_scope

from build_query.state import QueryState
from common_vars import get_tables_mapping
from models.env_variables import DUCKDB_PATH, BQ_TEST_PROJECT
from utils.errors import handle_compile_phase_exceptions, handle_post_compile_exceptions
from utils.saver import get_message_type

_bq_client: bigquery.Client | None = None


def _get_bq_client() -> bigquery.Client:
    global _bq_client
    if _bq_client is None:
        _bq_client = bigquery.Client(project=BQ_TEST_PROJECT)
    return _bq_client


async def evaluate(state: QueryState):
    code: str = state["query"]
    project = state["project"]
    dialect = state["dialect"]
    parent = state["messages"][-1].id if state.get("messages") else None
    return await validate_query(code, project, dialect, parent, state)


async def validate_query(code, project, dialect, parent, state):
    route = state.get("route", "").lower()
    try:
        await compile_query(code, project, dialect)
    except Exception as e:
        return handle_compile_phase_exceptions(
            exc=e, code=code, route=route, parent=parent, state=state
        )
    try:
        tables = await get_tables_mapping(project_id=project)

        optimized_sql, optimized, used_columns, literals = await evaluate_and_fix_query(
            code,
            project=project,
            mapping=tables,
            dialect=dialect,
            optimize=state.get("optimize", False),
        )
        ctes = await split_query(optimized, tables, dialect)
    except Exception as e:
        # Centralise la gestion des ParseError/OptimizeError/500
        return handle_post_compile_exceptions(exc=e, code=code)

        # Phase 3 : sortie standard
    formatted_used_columns = [json.dumps(x) for x in used_columns]
    used_columns_changed = has_used_column_changed(formatted_used_columns, state)

    return {
        "status": "success",
        "query_decomposed": json.dumps(ctes),
        "used_columns": formatted_used_columns,
        "used_columns_changed": used_columns_changed,
        "literals": literals,
        "optimized_sql": optimized_sql,
    }


def has_used_column_changed(formatted_used_columns, state):
    previous = state.get("used_columns")
    if not previous:
        return True
    return previous != formatted_used_columns


async def evaluate_and_fix_query(
    query, project, mapping=None, dialect="bigquery", optimize=False
):
    """
    Évalue et optimise une requête SQL en remplaçant les variables et en extrayant les informations nécessaires.
    """
    if mapping is None:
        mapping = await get_tables_mapping(project_id=project)

    parsed_statements = sqlglot.parse(query, read=dialect)

    final_query_ast = find_final_query_ast(parsed_statements)

    if final_query_ast:
        return await optimize_and_extract(
            final_query_ast, mapping, dialect, optimize=optimize
        )

    # Si aucune requête principale n'est trouvée
    return None, None, [], []


def extract_variables(parsed_statements):
    """Extrait les variables déclarées et assignées dans les statements."""
    variables = {}
    for statement in parsed_statements:
        if isinstance(statement, exp.Command) and statement.this.upper() == "DECLARE":
            match = re.match(
                r"DECLARE\s+(\w+)\s+\w+(?:\s+DEFAULT\s+['\"]?([^'\"]+)['\"]?)?",
                statement.sql(),
                re.IGNORECASE,
            )
            if match:
                variable_name, default_value = match.group(1), match.group(2)
                if default_value:
                    variables[variable_name] = default_value

        elif isinstance(statement, exp.Set):
            for set_item in statement.expressions:
                if isinstance(set_item, exp.SetItem) and isinstance(
                    set_item.this, exp.EQ
                ):
                    left_expr, right_expr = set_item.this.this, set_item.this.expression
                    if isinstance(left_expr, exp.Tuple) and isinstance(
                        right_expr, exp.Tuple
                    ):
                        for col_ast, val_ast in zip(
                            left_expr.expressions, right_expr.expressions
                        ):
                            if isinstance(col_ast, exp.Column) and isinstance(
                                val_ast, exp.Literal
                            ):
                                variables[col_ast.name] = val_ast.name
    return variables


def find_final_query_ast(parsed_statements) -> exp.Query | exp.With:
    """Identifie la requête principale dans les statements."""
    for statement in parsed_statements:
        if isinstance(statement, (exp.Query, exp.With)):
            return statement
    raise Exception("NO SQL QUERY FOUND")


async def optimize_and_extract(query_ast, mapping, dialect, optimize=False):
    """Optimise la requête et extrait les informations nécessaires."""
    result = await optimize_and_extract_info(
        query_ast, mapping, dialect, optimize=optimize
    )
    return (
        result["optimized_query"],
        result["optimized_ast"],
        result["used_columns"],
        result["literals"],
    )


# utils/compile_query.py (ou là où se trouve ta fonction)
async def compile_query(sql_code, project, dialect):
    if dialect == "bigquery":
        return run_query(sql_code, dry=True).total_bytes_processed

    elif dialect == "postgres":
        from utils.postgres_test_helper import query_on_test_dataset, PostgresTestHelper

        q = query_on_test_dataset(sql_code, project)
        pg_test = PostgresTestHelper()
        return await pg_test.run_query(sql=q, dry=True)

    elif dialect == "duckdb":
        from utils.duckdb_test_helper import query_on_test_dataset, DuckDBTestHelper

        q = query_on_test_dataset(sql_code, project)
        dk = DuckDBTestHelper(
            db_path=DUCKDB_PATH
        )  # ou un fichier pour partager entre runs
        try:
            return await dk.run_query(sql=q, dry=True)
        finally:
            await dk.close()
    else:
        raise ValueError(f"Unsupported dialect: {dialect}")


def run_query(sql, dry=True) -> bigquery.QueryJob:
    # Validate the query
    # This will raise an error if the query is not valid
    job_config = bigquery.QueryJobConfig(dry_run=dry, use_query_cache=False)
    query_job = _get_bq_client().query(sql, job_config=job_config)
    return query_job


def find_literals_and_columns(parsed_query: exp.Expression):
    """
    Finds all string and integer literals and their corresponding columns in a SQL query,
    scanning both WHERE and JOIN ... ON clauses.
    Returns a dict with:
      - 'columns_values': { column_name: [literal1, literal2, ...], ... }
      - 'predicate': the predicate expression (WHERE or ON) as SQL, if any literals were found
    """
    results = defaultdict(set)
    predicates = []

    def is_string_or_int_literal(expr):
        return isinstance(expr, exp.Literal) and (expr.is_string or expr.is_int)

    def process_condition(condition):
        # condition peut être un exp.Binary (EQ, AND, etc) ou un exp.In
        if not condition:
            return
        # tous les Binary (col = lit, lit = col, col <> lit, ...)
        for binary in condition.find_all(exp.Binary):
            lhs, r = binary.left, binary.right
            if isinstance(lhs, exp.Column) and is_string_or_int_literal(r):
                results[lhs.sql()].add(r.sql())
                predicates.append(condition.sql())
            elif is_string_or_int_literal(lhs) and isinstance(r, exp.Column):
                results[r.sql()].add(lhs.sql())
                predicates.append(condition.sql())
        # tous les IN (col IN ('a','b',...))
        for ino in condition.find_all(exp.In):
            col = ino.this
            if isinstance(col, exp.Column):
                for lit in ino.expressions:
                    if is_string_or_int_literal(lit):
                        results[col.sql()].add(lit.sql())
                        predicates.append(condition.sql())

    # 1) Traiter le WHERE
    where = parsed_query.find(exp.Where)
    if where:
        process_condition(where.this)

    # 2) Traiter chaque JOIN ... ON
    for join in parsed_query.find_all(exp.Join):
        on_cond = join.args.get("on")
        process_condition(on_cond)

    # Préparer le résultat
    # Enlever les guillemets des littéraux
    columns_values = {
        col: [v.strip("'\"") for v in literals] for col, literals in results.items()
    }

    output = {"columns_values": columns_values}
    if predicates:
        # On peut garder la première occurrence ou concaténer plusieurs
        output["predicate"] = predicates[0]

    return output


async def optimize_and_extract_info(parsed, tables, dialect, optimize=False):
    # Optimize the SQL query
    optimized = optimize_query(parsed, tables, dialect=dialect, optimize=optimize)

    literals = find_literals_and_columns(optimized)
    used_columns = await get_source_columns(optimized, tables)
    sql = optimized.sql(dialect=dialect, pretty=True)

    return {
        "used_columns": used_columns,
        "optimized_query": sql,
        "optimized_ast": optimized,
        # TODO workaround here to eliminate the int and float literals : find a better solution
        #  it will not work for something like CASE WHEN `orders`.`status` = 'Cancelled' THEN DATE('2020-01-01')
        #  here it will consider that the lit is status and the value is '2020-01-01'
        "literals": literals,
    }


async def get_source_columns(optimized, tables):
    all_columns_with_sources = get_all_columns_with_sources(optimized)

    used_columns = []
    for entry in all_columns_with_sources:
        table = entry["table"]
        project = entry["project"]
        database = entry["database"]
        used_columns_in_table = entry["used_columns"]

        used_identifiers = entry.get("used_identifiers", [])

        qualified = f"{database}.{table}" if database else table
        lookup_key = qualified if qualified in tables else table

        if lookup_key in tables:
            used_columns.append(
                {
                    "project": project,
                    "database": database,
                    "table": table,
                    "used_columns": sorted(used_columns_in_table),
                    "used_identifiers": used_identifiers,
                }
            )
    return used_columns


def prune_constant_group_by(expr: exp.Expression) -> exp.Expression:
    """
    Pour chaque SELECT de l'AST SQLGlot :
      - On identifie les projections sans colonne (constantes).
      - On retire du GROUP BY :
         * les positions ordinales vers ces constantes (en décalant les autres),
         * les références nominatives vers ces constantes.
      - On supprime ensuite ces projections constantes.
    """
    for scope in traverse_scope(expr):
        node = scope.expression
        if not isinstance(node, exp.Select):
            continue

        group = node.args.get("group")
        if group:
            # 1) Identifier les projections constantes
            const_positions = set()
            const_aliases = set()
            for idx, proj in enumerate(node.expressions, start=1):
                if proj.find(exp.Column) is None:
                    const_positions.add(idx)
                    const_aliases.add(proj.alias_or_name)

            # Rien à faire si pas de constante
            if not const_positions:
                continue

            new_group = []
            # On retirera toutes ces positions et noms
            drop_pos = const_positions
            drop_alias = const_aliases

            for g in group.expressions:
                # Cas positionnel : GROUP BY 1,2,...
                if isinstance(g, exp.Literal) and g.is_int:
                    pos = int(g.this)
                    if pos in drop_pos:
                        # on supprime cette entrée
                        continue
                    new_group.append(exp.Literal.number(str(pos)))
                # Cas nominatif : GROUP BY alias
                elif isinstance(g, exp.Column) and g.alias_or_name in drop_alias:
                    # on supprime cette référence
                    continue
                else:
                    # tout autre GROUP BY on le conserve
                    new_group.append(g)

            # On applique la nouvelle clause GROUP BY (ou on la vire si vide)
            if new_group:
                node.set("group", exp.Group(expressions=new_group))
            else:
                node.set("group", None)

    return expr


def optimize_query(parsed, tables, dialect="bigquery", optimize=False):
    from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
    from sqlglot.optimizer.qualify_columns import qualify_columns
    from sqlglot.optimizer.qualify_tables import qualify_tables

    schema = MappingSchema()
    for table_name, columns in tables.items():
        schema.add_table(table_name, columns, dialect=dialect)

    parsed = normalize_identifiers(parsed, dialect=dialect)

    if optimize:
        try:
            pre_optimize = prune_constant_group_by(parsed)
        except Exception:
            pre_optimize = parsed
        try:
            return sqlglot.optimizer.optimize(
                pre_optimize, schema=schema, dialect=dialect
            )
        except Exception as e:
            print(f"Optimisation complète échouée ({e}), fallback sur qualify.")
            parsed = pre_optimize

    expr = qualify_tables(parsed)
    return qualify_columns(expr, schema, infer_schema=True)


def get_all_columns_with_sources(sql_expression):
    col_with_sources = {}

    def extract_columns_from_scope(scope):
        for column in scope.columns:
            table_alias = column.table
            project_text = None
            database_text = None
            table_name_text = None
            alias_text = None

            if table_alias in scope.sources:
                source = scope.sources[table_alias]

                # On ignore les fausses tables créées par les UNNEST
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
            else:
                table_name_text = table_alias
                alias_text = table_alias

            # Sécurité pour ne pas stocker de sources vides
            if not table_name_text:
                continue

            key = (project_text, database_text, table_name_text)
            if key not in col_with_sources:
                col_with_sources[key] = {
                    "project": project_text,
                    "database": database_text,
                    "table": table_name_text,
                    "alias": alias_text,
                    "used_columns": set(),
                }

            col_with_sources[key]["used_columns"].add(column.name.lower())

    # 1. Parcours classique des scopes
    scopes = traverse_scope(sql_expression)
    for scope in scopes:
        extract_columns_from_scope(scope)

    # 2. L'APPROCHE CHIRURGICALE : On extrait uniquement les morceaux de colonnes
    global_columns = set()
    for col in sql_expression.find_all(exp.Column):
        # col.parts sépare les éléments (ex: hits.product.v2productname -> ['hits', 'product', 'v2productname'])
        if hasattr(col, "parts"):
            for part in col.parts:
                global_columns.add(part.name.lower())
        else:
            global_columns.add(col.name.lower())

    # 3. Formatage
    result = []
    for _, info in sorted(
        col_with_sources.items(), key=lambda x: (x[0][0] or "", x[0][1] or "", x[0][2])
    ):
        info["used_columns"] = list(info["used_columns"])
        info["used_identifiers"] = list(global_columns)
        result.append(info)

    return result


def find_columns_used(data):
    columns_used = []
    for table, records in data.items():
        if records:
            # Get columns from the first record
            columns = list(records[0].keys())
            table_info = {"table": table, "used_columns": sorted(columns)}
            columns_used.append(table_info)
    return columns_used


def extract_cte_dependencies(cte_name, parsed_sql, all_ctes):
    """
    Improved helper function to identify dependencies of a CTE using sqlglot.
    It parses the SQL and extracts the exact table/CTE references.
    """
    dependencies = []

    # Get all table/CTE references in the SQL
    referenced_tables = {table.name for table in parsed_sql.find_all(sqlglot.exp.Table)}

    # Check if the references match any of the CTE names
    for other_cte in all_ctes:
        other_cte_name = other_cte.alias_or_name
        if other_cte_name != cte_name and other_cte_name in referenced_tables:
            dependencies.append(other_cte_name)

    return dependencies


async def split_query(
    sql_expression: sqlglot.expressions.Query, source_tables, dialect
):
    """
    Function that splits the SQL query into multiple CTEs using sqlglot,
    identifies the dependencies between them, and adds a final step named `final_query`.
    """
    # Initialize list to hold CTEs
    ctes = []
    # Extract the WITH clause
    with_clause = sql_expression.ctes
    if with_clause:
        # Loop through each CTE in the WITH clause
        for cte in with_clause:
            cte_name = cte.alias_or_name  # Get the name of the CTE
            # Extract only the SELECT statement without the alias
            cte_select = cte.this.sql(
                dialect=dialect, pretty=True
            )  # Get only the inner SELECT statement
            # Find dependencies for this CTE
            dependencies = extract_cte_dependencies(cte_name, cte, with_clause)
            sources = await get_source_columns(cte.this, source_tables)
            # Append the CTE to the list
            ctes.append(
                {
                    "name": cte_name,
                    "code": cte_select,
                    "dependencies": dependencies,
                    "sources": sources,
                }
            )

    sql_expression.ctes.clear()
    # Extract the final query (after the WITH clause)
    final_query = sql_expression.sql(dialect=dialect, pretty=True)
    final_query_expr = sqlglot.parse_one(
        remove_with_start(final_query), dialect=dialect
    )
    sources = await get_source_columns(final_query_expr, source_tables)
    # Add the final query step
    ctes.append(
        {
            "name": "final_query",
            "code": remove_with_start(final_query),
            "dependencies": [cte["name"] for cte in ctes] if ctes else [],
            "sources": sources,
        }
    )
    return ctes


def remove_with_start(input_string):
    import re

    # Strip leading SQL comments (-- ... and /* ... */) before checking for WITH
    stripped = re.sub(
        r"(--[^\n]*\n|/\*.*?\*/)", "", input_string, flags=re.DOTALL
    ).strip()
    if stripped.lower().startswith("with"):
        # Find where 'with' starts in the original string and remove from there
        match = re.search(r"\bwith\b", input_string, flags=re.IGNORECASE)
        if match:
            return input_string[match.start() + 4 :].strip()
    return input_string


def find_last_sql_message(solver_messages) -> AIMessage | None:
    for message in reversed(solver_messages):
        if get_message_type(message) == "sql":
            return message
    return None
