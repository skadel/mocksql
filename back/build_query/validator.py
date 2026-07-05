import asyncio
import json
import logging
import re
from collections import defaultdict
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from google.cloud import bigquery

import sqlglot
from sqlglot._typing import E
from langchain_core.messages import AIMessage
from sqlglot import MappingSchema
from sqlglot import expressions as exp
from sqlglot.optimizer.scope import traverse_scope

from build_query.path_slicer import build_path_plans
from build_query.state import QueryState
from common_vars import get_tables_mapping
from models.env_variables import DUCKDB_PATH, BQ_TEST_PROJECT
from utils.errors import handle_compile_phase_exceptions, handle_post_compile_exceptions
from utils.saver import get_message_type
from utils.sql_code import get_all_columns_with_sources
from utils.timing import atimed, timed

logger = logging.getLogger(__name__)

_bq_client: "bigquery.Client | None" = None


def _get_bq_client() -> "bigquery.Client":
    from utils.optional_deps import import_bigquery

    bigquery = import_bigquery()
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


def _normalize_column_qualifiers(sql: str, dialect: str) -> str:
    """Réécrit les qualificateurs de colonne `dataset.table`.col en `alias`.col.

    BigQuery traite une table écrite en chemin pointé entre backticks
    (``\\`dataset.table\\``` ou ``\\`project.dataset.table\\```) comme un identifiant
    unique dont l'alias implicite est **le chemin entier**, pas le dernier segment.
    Du coup ``\\`dataset.table\\`.col`` fonctionne dans la console mais l'API rejette
    aussi bien ``\\`dataset.table\\`.col`` (« Unrecognized name: \\`dataset.table\\` »)
    que le segment final ``table.col`` (« Unrecognized name: table ») dès qu'un
    project est préfixé en amont (_qualify_two_part_refs) — le chemin implicite a
    alors changé.

    Plutôt que de deviner l'alias implicite (fragile), on donne à la table un
    **alias explicite** (son dernier segment, désambiguïsé si collision) et on
    réécrit chaque qualificateur de colonne vers cet alias. C'est la même
    stratégie que le chemin DuckDB (``strip_qualifiers_with_scope``).
    """
    try:
        from sqlglot.optimizer.scope import traverse_scope

        tree = sqlglot.parse(sql, read=dialect)
        if not tree:
            return sql
        root = tree[0]
        for scope in traverse_scope(root):
            # On matche sur (db, table) en IGNORANT le catalog : un qualificateur
            # de colonne `dataset.table`.col ne porte jamais le project, alors que
            # la table source peut avoir été enrichie d'un catalog en amont.
            base_tables: dict[tuple, exp.Table] = {}
            # scope.sources est indexé par nom/alias de source (les valeurs peuvent
            # être des Scope pour les CTE/sous-requêtes, pas seulement des Table).
            used_aliases: set[str] = set(scope.sources.keys())
            for source in scope.sources.values():
                if isinstance(source, exp.Table) and source.db:
                    # Clé EN MINUSCULE : sqlglot peut écrire la table en casse d'origine
                    # et le qualificateur de colonne en casse normalisée — comparer en
                    # minuscule évite de rater le rapprochement (BigQuery/DuckDB sont
                    # insensibles à la casse).
                    base_tables[(source.db.lower(), source.this.name.lower())] = source

            assigned: dict[int, str] = {}  # id(table) -> alias retenu
            for col in scope.expression.find_all(exp.Column):
                col_db = col.text("db")
                if not col_db:
                    continue
                table = base_tables.get((col_db.lower(), col.text("table").lower()))
                if table is None:
                    continue

                alias = table.alias or assigned.get(id(table))
                if not alias:
                    base = table.this.name
                    # La clé implicite de la table (son dernier segment) est déjà
                    # dans used_aliases : ne pas la compter comme collision avec
                    # elle-même (`FROM x.y.tbl AS tbl` est valide).
                    conflicts = used_aliases - {table.alias_or_name}
                    alias = base
                    n = 1
                    while alias in conflicts:
                        n += 1
                        alias = f"{base}_{n}"
                    used_aliases.add(alias)
                    table.set("alias", exp.TableAlias(this=exp.to_identifier(alias)))
                    assigned[id(table)] = alias

                col.set("catalog", None)
                col.set("db", None)
                col.set("table", exp.to_identifier(alias))
        return root.sql(dialect=dialect)
    except Exception:
        return sql


async def validate_query(code, project, dialect, parent, state):
    route = state.get("route", "").lower()
    try:
        # BigQuery API ne supporte pas `dataset.table`.col comme qualificateur de
        # colonne — normaliser avant le dry-run sans modifier la sémantique.
        compile_code = _normalize_column_qualifiers(code, dialect)
        await compile_query(compile_code, project, dialect)
    except Exception as e:
        return handle_compile_phase_exceptions(
            exc=e, code=code, route=route, parent=parent, state=state
        )
    try:
        async with atimed("validate: extraction sqlglot (optimize + split)"):
            async with atimed("validate:   get_tables_mapping"):
                tables = await get_tables_mapping(project_id=project)

            async with atimed("validate:   evaluate_and_fix_query"):
                (
                    optimized_sql,
                    optimized,
                    used_columns,
                    literals,
                ) = await evaluate_and_fix_query(
                    code,
                    mapping=tables,
                    dialect=dialect,
                    optimize=state.get("optimize", False),
                )
            async with atimed("validate:   split_query"):
                ctes = await split_query(optimized, tables, dialect)
    except Exception as e:
        # Centralise la gestion des ParseError/OptimizeError/500
        return handle_post_compile_exceptions(exc=e, code=code)

        # Phase 3 : sortie standard
    formatted_used_columns = [json.dumps(x) for x in used_columns]
    used_columns_changed = has_used_column_changed(formatted_used_columns, state)

    # Catalogue des paths UNION ALL (AST pur sur le SQL déjà validé/décomposé — pas de
    # re-dry-run ni ré-extraction de colonnes). None si pas d'UNION ALL de 1er niveau
    # exploitable → comportement inchangé. Recalculé à chaque validation (donc à jour
    # si le SQL change). `optimized_sql` reste la requête COMPLÈTE (jamais le slicé).
    try:
        path_plans = build_path_plans(optimized_sql, ctes, used_columns, dialect)
    except Exception:
        logger.warning(
            "build_path_plans a échoué (sql=%s) — fallback path 'all'",
            optimized_sql,
            exc_info=True,
        )
        path_plans = None

    return {
        "status": "success",
        "query_decomposed": json.dumps(ctes),
        "used_columns": formatted_used_columns,
        "used_columns_changed": used_columns_changed,
        "literals": literals,
        "optimized_sql": optimized_sql,
        "path_plans": json.dumps(path_plans) if path_plans else None,
    }


def has_used_column_changed(formatted_used_columns, state):
    previous = state.get("used_columns")
    if not previous:
        return True
    return previous != formatted_used_columns


async def evaluate_and_fix_query(query, mapping, dialect="bigquery", optimize=False):
    """
    Évalue et optimise une requête SQL en remplaçant les variables et en extrayant les informations nécessaires.
    """
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


async def optimize_and_extract(query_ast: E, mapping, dialect, optimize=False):
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
        with timed("validate: dry-run BigQuery"):
            return run_query(sql_code, dry=True).total_bytes_processed

    elif dialect == "postgres":
        from utils.postgres_test_helper import query_on_test_dataset, PostgresTestHelper

        q = query_on_test_dataset(sql_code, project)
        pg_test = PostgresTestHelper()
        async with atimed("validate: dry-run Postgres"):
            return await pg_test.run_query(sql=q, dry=True)

    elif dialect == "duckdb":
        from utils.duckdb_test_helper import query_on_test_dataset, DuckDBTestHelper

        q = query_on_test_dataset(sql_code, project)
        dk = DuckDBTestHelper(
            db_path=DUCKDB_PATH
        )  # ou un fichier pour partager entre runs
        try:
            async with atimed("validate: dry-run DuckDB"):
                return await dk.run_query(sql=q, dry=True)
        finally:
            await dk.close()

    elif dialect == "snowflake":
        from utils.snowflake_connector import run_sf_query

        async with atimed("validate: dry-run Snowflake"):
            await asyncio.to_thread(run_sf_query, sql_code, True)
        return 0

    elif dialect == "trino":
        from utils.trino_connector import run_trino_query

        async with atimed("validate: dry-run Trino"):
            await asyncio.to_thread(run_trino_query, sql_code, True)
        return 0

    else:
        raise ValueError(f"Unsupported dialect: {dialect}")


def run_query(sql, dry=True) -> "bigquery.QueryJob":
    from utils.optional_deps import import_bigquery

    bigquery = import_bigquery()
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


async def optimize_and_extract_info(parsed: E, tables, dialect, optimize=False):
    from build_query.scalar_folder import fold_scalar_expressions

    # Optimize the SQL query
    with timed("validate:     optimize_query"):
        optimized = optimize_query(parsed, tables, dialect=dialect, optimize=optimize)
    with timed("validate:     fold_scalar_expressions"):
        optimized = fold_scalar_expressions(optimized, source_dialect=dialect)

    with timed("validate:     find_literals_and_columns"):
        literals = find_literals_and_columns(optimized)
    async with atimed("validate:     get_source_columns"):
        used_columns = await get_source_columns(optimized, tables)
    with timed("validate:     render optimized.sql(pretty)"):
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
    all_columns_with_sources = get_all_columns_with_sources(
        optimized, schema_mapping=tables
    )

    used_columns = []
    for entry in all_columns_with_sources:
        table = entry["table"]
        project = entry["project"]
        database = entry["database"]
        used_columns_in_table = entry["used_columns"]

        used_identifiers = entry.get("used_identifiers", [])

        qualified = f"{database}.{table}" if database else table
        lookup_key = qualified if qualified in tables else table

        in_mapping = lookup_key in tables
        # Include if found in schema mapping, OR if it's a real external table (has a database prefix).
        # Without this fallback, an empty schema cache would filter out all real tables.
        is_real_table = bool(database)

        if in_mapping or is_real_table:
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
            if (
                group.args.get("all")
                or group.args.get("rollup")
                or group.args.get("cube")
                or group.args.get("grouping_sets")
            ):
                continue

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


def expand_positional_group_by(expr: exp.Expression) -> exp.Expression:
    """Réécrit les références ordinales du GROUP BY (`GROUP BY 1, 2`) en copies des
    expressions du SELECT correspondantes, afin qu'aucun élagage de projection en
    aval ne puisse laisser un ordinal hors-plage — DuckDB lèverait alors
    « Binder Error: GROUP BY term out of range ».

    Normalement ``qualify_columns`` fait déjà cette expansion ; cette passe est un
    filet de sécurité pour les chemins où le positionnel survit (qualify partiel,
    repli sur le SQL brut quand l'optimisation lève). À exécuter **avant** tout
    pruning (pushdown_projections).

    Garde-fous :
      - on ignore les SELECT dont la projection contient encore une étoile non
        expansée (les positions y sont ambiguës) ;
      - on ignore GROUP BY ALL / ROLLUP / CUBE / GROUPING SETS ;
      - une projection purement constante (sans colonne) est un no-op de groupage :
        on retire l'ordinal plutôt que d'émettre un littéral (un `GROUP BY 5`
        littéral serait ré-interprété comme ordinal par DuckDB) ;
      - un ordinal déjà hors-plage (corrompu en amont) est retiré avec un warning.
    """
    for select in expr.find_all(exp.Select):
        group = select.args.get("group")
        if not group:
            continue
        if (
            group.args.get("all")
            or group.args.get("rollup")
            or group.args.get("cube")
            or group.args.get("grouping_sets")
        ):
            continue

        projections = select.expressions

        # On ignore les SELECT contenant une étoile de projection NON expansée
        # (`SELECT *`, `t.*`) : les positions y sont ambiguës. Attention : une étoile
        # imbriquée dans un agrégat (`COUNT(*)`) n'est PAS une projection-étoile.
        def _is_star_projection(p: exp.Expression) -> bool:
            node = p.this if isinstance(p, exp.Alias) else p
            return isinstance(node, exp.Star) or (
                isinstance(node, exp.Column) and isinstance(node.this, exp.Star)
            )

        if any(_is_star_projection(p) for p in projections):
            continue

        new_group: list[exp.Expression] = []
        changed = False
        for g in group.expressions:
            if isinstance(g, exp.Literal) and g.is_int:
                pos = int(g.this)
                if not (1 <= pos <= len(projections)):
                    logger.warning(
                        "expand_positional_group_by: ordinal %s hors-plage (1..%s), retiré.",
                        pos,
                        len(projections),
                    )
                    changed = True
                    continue
                proj = projections[pos - 1]
                target = proj.this if isinstance(proj, exp.Alias) else proj
                changed = True
                if target.find(exp.Column) is None:
                    # projection constante → groupage sans effet, on retire l'ordinal
                    continue
                new_group.append(target.copy())
            else:
                new_group.append(g)

        if changed:
            if new_group:
                select.set("group", exp.Group(expressions=new_group))
            else:
                select.set("group", None)

    return expr


def _build_identifier_case_map(parsed, tables) -> dict[str, str]:
    """Construit une table {nom_en_lowercase -> casse_d'origine} pour restaurer la
    casse écrite par l'utilisateur après ``normalize_identifiers`` (qui lowercase
    tout). La casse écrite dans la requête prime sur celle du schéma."""
    case_map: dict[str, str] = {}
    # 1) Casse canonique du schéma (tables + colonnes) — fallback.
    for table_name, columns in tables.items():
        for part in str(table_name).split("."):
            case_map.setdefault(part.lower(), part)
        for col in columns:
            case_map.setdefault(col.lower(), col)
    # 2) Casse écrite par l'utilisateur — prioritaire (écrase le schéma).
    for ident in parsed.find_all(exp.Identifier):
        if not ident.quoted:
            case_map[ident.this.lower()] = ident.this
    return case_map


def _restore_identifier_case(expr, case_map: dict[str, str]):
    """Réapplique la casse d'origine sur les identifiants non-quotés. Indispensable
    car ``normalize_identifiers`` lowercase tout : sur un UNPIVOT le nom de colonne
    devient une *valeur* de sortie (`Jan` → `'Jan'`), et BigQuery préserve la casse.
    Les identifiants inconnus de la map (alias générés par qualify) sont laissés."""
    for ident in expr.find_all(exp.Identifier):
        if ident.quoted:
            continue
        original = case_map.get(ident.this.lower())
        if original and original != ident.this:
            ident.set("this", original)
    return expr


def optimize_query(parsed, tables, dialect="bigquery", optimize=False):
    from sqlglot.optimizer.normalize_identifiers import normalize_identifiers
    from sqlglot.optimizer.qualify_columns import qualify_columns
    from sqlglot.optimizer.qualify_tables import qualify_tables

    with timed("validate:       build MappingSchema"):
        schema = MappingSchema()
        for table_name, columns in tables.items():
            schema.add_table(table_name, columns, dialect=dialect)

    # Casse d'origine capturée AVANT normalize_identifiers, restaurée en sortie.
    case_map = _build_identifier_case_map(parsed, tables)

    with timed("validate:       normalize_identifiers"):
        parsed = normalize_identifiers(parsed, dialect=dialect)

    if optimize:
        try:
            pre_optimize = prune_constant_group_by(parsed)
            pre_optimize = expand_positional_group_by(pre_optimize)
        except Exception:
            pre_optimize = parsed
        try:
            return _restore_identifier_case(
                sqlglot.optimizer.optimize(
                    pre_optimize, schema=schema, dialect=dialect
                ),
                case_map,
            )
        except Exception as e:
            logger.warning(
                "Optimisation complète échouée (%s), fallback sur qualify.", e
            )
            parsed = pre_optimize

    from utils.examples import _fix_unnest_alias_conflicts, _fix_unnest_scope_leak
    from sqlglot.optimizer.pushdown_projections import pushdown_projections
    from sqlglot.optimizer.simplify import simplify

    # Minimum requis : qualifier tables + colonnes. Sans ça, pas d'extraction
    # fiable des colonnes — une exception ici remonte (gérée en amont).
    with timed("validate:       qualify_tables"):
        expr = qualify_tables(parsed)
    with timed("validate:       qualify_columns(infer_schema)"):
        expr = qualify_columns(expr, schema, infer_schema=True)

    # Passes d'optimisation appliquées **une par une, chacune isolée dans son
    # propre try/except** : on prend le maximum d'optimisations possibles, et si
    # une passe casse on l'ignore et on garde l'expression de l'étape précédente
    # (fallback progressif vers le minimum = qualify). Toutes préservent les CTEs.
    #
    # Volontairement EXCLUES (l'optimiseur complet de sqlglot les ferait) :
    #  - merge_subqueries / eliminate_ctes / eliminate_subqueries / eliminate_joins
    #    → fusionnent ou suppriment des CTEs, or split_query décompose les tests
    #      par CTE et en dépend ;
    #  - normalize / pushdown_predicates → réécrivent les prédicats en CNF/DNF, à
    #    blow-up exponentiel sur les WHERE complexes (contraire à l'objectif latence).
    #
    # Effets recherchés :
    #  - pushdown_projections élague les colonnes jamais consommées en aval (crucial
    #    sur les `SELECT *` de tables larges qui gonflent used_columns + profiling) ;
    #  - simplify replie les constantes nativement (1*5 → 5, booléens), allégeant
    #    d'autant fold_scalar_expressions en aval.
    passes = [
        ("_fix_unnest_alias_conflicts", _fix_unnest_alias_conflicts),
        ("_fix_unnest_scope_leak", _fix_unnest_scope_leak),
        # Filet de sécurité AVANT pushdown : si qualify a laissé des ordinaux dans le
        # GROUP BY (étoile non expansée…), on les binde aux expressions du SELECT pour
        # que l'élagage de projection ne crée pas un « GROUP BY out of range ».
        ("expand_positional_group_by", expand_positional_group_by),
        (
            "pushdown_projections",
            lambda e: pushdown_projections(e, schema, dialect=dialect),
        ),
        ("simplify", lambda e: simplify(e, dialect=dialect)),
    ]
    for name, fn in passes:
        with timed(f"validate:       {name}"):
            try:
                expr = fn(expr)
            except Exception:
                logger.warning("optimize_query: passe '%s' ignorée (best-effort)", name)

    return _restore_identifier_case(expr, case_map)


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

    # Extract inline subqueries (FROM (SELECT ...) AS alias) as inspectable steps
    existing_names = {c["name"] for c in ctes}
    for node in final_query_expr.walk():
        if (
            isinstance(node, sqlglot.exp.Subquery)
            and node.alias
            and node.alias not in existing_names
        ):
            existing_names.add(node.alias)
            ctes.append(
                {
                    "name": node.alias,
                    "code": node.this.sql(dialect=dialect, pretty=True),
                    "dependencies": [],
                    "sources": [],
                }
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
