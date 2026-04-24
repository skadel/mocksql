import re

import sqlglot
from sqlglot import MappingSchema
from sqlglot import expressions as exp
from sqlglot import optimizer


def determine_query_grain(query, tables_and_columns, dialect="bigquery"):
    parsed_query = sqlglot.parse_one(query, dialect=dialect)
    schema = create_mapping_schema(tables_and_columns)
    optimized_query = optimizer.optimize(parsed_query, schema=schema, dialect=dialect)

    cte_grain_map = {}
    column_aliases = extract_column_aliases(optimized_query)

    try:
        grains = find_grain(
            optimized_query, tables_and_columns, column_aliases, cte_grain_map
        )
    except Exception as e:
        print("Exception in find_grain")
        print(e)
        grains = None

    return {
        "grains": grains,
        "result_columns": column_aliases,
        "cte_grain_map": cte_grain_map,
    }


def create_mapping_schema(tables_and_columns):
    schema = MappingSchema()
    for table in tables_and_columns:
        table_name = table["table_name"].split(".")[-1]
        columns = {col["name"]: col["type"] for col in table["columns"]}
        schema.add_table(table_name, columns)
    return schema


def map_aliases(select_expressions, from_expr=None, join_exprs=None):
    alias_map = {}
    reverse_alias_map = {}
    table_alias_map = {}
    reverse_table_alias_map = {}

    # Step 1: Process FROM and JOIN clauses to build the table alias map
    if from_expr:
        from_table = from_expr.find(exp.Table)
        if from_table:
            table_name = from_table.name
            table_alias = from_table.alias_or_name
            # Map both the alias and the original table name
            table_alias_map[table_alias] = table_name
            reverse_table_alias_map[table_name] = table_alias
            table_alias_map[table_name] = table_name

    if join_exprs:
        for join_expr in join_exprs:
            join_table = join_expr.this.find(exp.Table)
            if join_table:
                table_name = join_table.name
                table_alias = join_table.alias_or_name
                # Map both the alias and the original table name
                table_alias_map[table_alias] = table_name
                table_alias_map[table_name] = table_name
                reverse_table_alias_map[table_name] = table_alias

    # Step 2: Map column aliases and reverse aliases in the SELECT clause
    for expr in select_expressions:
        if isinstance(expr, exp.Alias):
            original_expr = expr.this
            if isinstance(original_expr, exp.Column):
                table_name = original_expr.table
                col_name = original_expr.name
                table_name_mapped = table_alias_map.get(table_name, table_name)
                table_alias = table_name

                # Add mappings for both (table_name, col_name) and (table_alias, col_name)
                alias_map[(table_name_mapped, col_name)] = expr.alias
                reverse_alias_map[expr.alias] = (table_name_mapped, col_name)

                if table_alias:
                    alias_map[(table_alias, col_name)] = expr.alias
                    reverse_alias_map[expr.alias] = (table_alias, col_name)

            elif isinstance(original_expr, exp.Func):
                alias_map[original_expr.sql()] = expr.alias
                reverse_alias_map[expr.alias] = original_expr.sql()

        elif isinstance(expr, exp.Column):
            table_name = expr.table
            col_name = expr.name
            table_name_mapped = table_alias_map.get(table_name, table_name)
            table_alias = (
                table_name_mapped if table_name in table_alias_map else table_name
            )

            # Add mappings for both (table_name, col_name) and (table_alias, col_name)
            alias_map[(table_name_mapped, col_name)] = expr.name
            reverse_alias_map[expr.name] = (table_name_mapped, col_name)

            if table_alias:
                alias_map[(table_alias, col_name)] = expr.name
                reverse_alias_map[expr.name] = (table_alias, col_name)

    return alias_map, reverse_alias_map, table_alias_map, reverse_table_alias_map


def extract_column_name(expr):
    if isinstance(expr, exp.Alias):
        return expr.alias
    if isinstance(expr, exp.Column):
        return expr.name
    if isinstance(expr, exp.Cast):
        return expr.name
    return None


def process_with_clause(node, tables_and_columns, cte_grain_map):
    with_clause = node.args.get("with_")
    if with_clause:
        for cte in with_clause.expressions:
            cte_name = cte.alias_or_name

            # Get the SELECT statement inside the CTE
            cte_expression = cte.args.get("this")

            if isinstance(cte_expression, (exp.Select, exp.Union)):
                column_aliases = extract_column_aliases(cte_expression)
                # Use the find_grain function to determine the grain of the CTE's SELECT statement
                cte_grain = find_grain(
                    cte_expression, tables_and_columns, column_aliases, cte_grain_map
                )
                # Store the grain in the CTE grain map
                cte_grain_map[cte_name] = cte_grain
            else:
                # Raise an error if the CTE does not contain a valid SELECT statement
                raise ValueError(
                    f"CTE '{cte_name}' does not contain a valid SELECT statement."
                )
    return cte_grain_map


def process_group_by(
    node, tables_and_columns, cte_grain_map, table_alias_map, reverse_table_alias_map
):
    group_by = node.args.get("group")

    if group_by:
        select_expressions = node.args.get("expressions", [])
        local_alias_map, _, _, _ = map_aliases(select_expressions)
        grain_columns = set()

        table_to_columns_map = {}
        alias_to_table_map = {}

        for col in group_by.expressions:
            if isinstance(col, exp.Column):
                col_name = extract_column_name(col)
                table_name = col.table
                original_table_name = table_alias_map.get(table_name, table_name)
                fully_qualified_col = (table_name, col_name)
                alias_name = local_alias_map.get(fully_qualified_col, col_name)
                grain_columns.add(alias_name)
                if original_table_name not in table_to_columns_map:
                    table_to_columns_map[original_table_name] = set()
                table_to_columns_map[original_table_name].add(alias_name)

            elif isinstance(col, exp.Alias) and isinstance(col.this, exp.Column):
                table_name = col.this.table
                original_table_name = table_alias_map.get(table_name, table_name)
                alias_name = col.alias
                grain_columns.add(alias_name)
                if original_table_name not in table_to_columns_map:
                    table_to_columns_map[original_table_name] = set()
                table_to_columns_map[original_table_name].add(alias_name)
                alias_to_table_map[alias_name] = original_table_name

            elif isinstance(col, exp.Alias) and isinstance(col.this, exp.Func):
                alias_name = col.alias
                grain_columns.add(alias_name)

            elif isinstance(col, exp.Func):
                func_sql = col.sql()
                cleaned_f = clean_column_name(func_sql)
                alias_name = local_alias_map.get(cleaned_f) or local_alias_map.get(
                    func_sql, func_sql
                )
                grain_columns.add(alias_name)

        # Now check if any table or CTE has its primary keys and additional columns in the grain
        final_grain = set(grain_columns)
        for table_name, columns in table_to_columns_map.items():
            primary_keys = cte_grain_map.get(table_name) or get_primary_keys(
                table_name, tables_and_columns
            )

            if primary_keys:
                primary_keys_in_grain = {
                    local_alias_map.get((table_name, pk), pk) for pk in primary_keys
                }
                alias_primary_keys_in_grain = {
                    local_alias_map.get(
                        (reverse_table_alias_map.get(table_name), pk), pk
                    )
                    for pk in primary_keys
                }

                if primary_keys_in_grain.intersection(
                    columns
                ) or alias_primary_keys_in_grain.intersection(columns):
                    # If primary keys or their aliases are present with other columns, only keep the primary keys
                    final_grain -= columns
                    final_grain.update(
                        primary_keys_in_grain
                    ) if primary_keys_in_grain.intersection(
                        columns
                    ) else final_grain.update(alias_primary_keys_in_grain)

        return sorted(final_grain)

    return None


def process_distinct(
    node, tables_and_columns, cte_grain_map, table_alias_map, reverse_table_alias_map
):
    distinct = node.args.get("distinct")

    if distinct:
        select_expressions = node.args.get("expressions", [])
        local_alias_map, _, _, _ = map_aliases(select_expressions)
        distinct_columns = set()

        table_to_columns_map = {}
        alias_to_table_map = {}

        for col in select_expressions:
            if isinstance(col, exp.Column):
                col_name = extract_column_name(col)
                table_name = col.table
                original_table_name = table_alias_map.get(table_name, table_name)
                fully_qualified_col = (table_name, col_name)
                alias_name = local_alias_map.get(fully_qualified_col, col_name)
                distinct_columns.add(alias_name)
                if original_table_name not in table_to_columns_map:
                    table_to_columns_map[original_table_name] = set()
                table_to_columns_map[original_table_name].add(alias_name)

            elif isinstance(col, exp.Alias) and isinstance(col.this, exp.Column):
                table_name = col.this.table
                original_table_name = table_alias_map.get(table_name, table_name)
                alias_name = col.alias
                distinct_columns.add(alias_name)
                if original_table_name not in table_to_columns_map:
                    table_to_columns_map[original_table_name] = set()
                table_to_columns_map[original_table_name].add(alias_name)
                alias_to_table_map[alias_name] = original_table_name

            elif isinstance(col, exp.Alias) and isinstance(col.this, exp.Func):
                alias_name = col.alias
                distinct_columns.add(alias_name)

            elif isinstance(col, exp.Func):
                func_sql = col.sql()
                cleaned_f = clean_column_name(func_sql)
                alias_name = local_alias_map.get(cleaned_f) or local_alias_map.get(
                    func_sql, func_sql
                )
                distinct_columns.add(alias_name)

        # Now check if any table or CTE has its primary keys and additional columns in the distinct
        final_distinct = set(distinct_columns)
        for table_name, columns in table_to_columns_map.items():
            primary_keys = cte_grain_map.get(table_name) or get_primary_keys(
                table_name, tables_and_columns
            )

            if primary_keys:
                primary_keys_in_distinct = {
                    local_alias_map.get((table_name, pk), pk) for pk in primary_keys
                }
                alias_primary_keys_in_distinct = {
                    local_alias_map.get(
                        (reverse_table_alias_map.get(table_name), pk), pk
                    )
                    for pk in primary_keys
                }

                if primary_keys_in_distinct.intersection(
                    columns
                ) or alias_primary_keys_in_distinct.intersection(columns):
                    # If primary keys or their aliases are present with other columns, only keep the primary keys
                    final_distinct -= columns
                    final_distinct.update(
                        primary_keys_in_distinct
                    ) if primary_keys_in_distinct.intersection(
                        columns
                    ) else final_distinct.update(alias_primary_keys_in_distinct)

        return sorted(final_distinct)

    return None


def find_column_references(expr, table_name=None):
    """
    Recursively find all column references within an expression, optionally filtering by table name.
    """
    references = set()

    if isinstance(expr, exp.Column):
        if table_name is None or expr.table == table_name:
            references.add(expr.name)
    elif isinstance(expr, exp.Alias):
        references.update(find_column_references(expr.this, table_name))
    elif hasattr(expr, "expressions"):
        for sub_expr in expr.expressions:
            references.update(find_column_references(sub_expr, table_name))

    return references


def extract_columns(expression):
    """Recursively extract table aliases and column names from join conditions."""
    columns = []

    if isinstance(expression, exp.Column):
        # Base case: if the expression is a Column, extract the table alias and column name
        table_alias = expression.table if expression.table else None
        column_name = expression.this.this
        columns.append((table_alias, column_name))
    elif isinstance(expression, exp.Binary):
        # If it's a Binary operation (e.g., EQ, AND, OR), extract from both sides
        columns.extend(extract_columns(expression.this))
        columns.extend(extract_columns(expression.expression))
    elif isinstance(expression, exp.And) or isinstance(expression, exp.Or):
        # If it's an AND/OR operation, treat both sides as potentially complex expressions
        columns.extend(extract_columns(expression.this))
        columns.extend(extract_columns(expression.expression))
    elif isinstance(expression, exp.Func):
        # If the expression is a function, extract columns from its arguments
        for arg in expression.args:
            columns.extend(extract_columns(arg))

    return columns


def preprocess_alias_map(alias_map):
    """Preprocess alias_map by cleaning its keys and ensuring the correct structure."""
    cleaned_alias_map = {}
    for key, value in alias_map.items():
        # If the key is a SQL expression string, attempt to extract the table and column name
        if isinstance(key, str):
            # Remove functions and other SQL expressions
            cleaned_key = clean_column_name(key)

            # Attempt to extract table name and column name from the cleaned key
            table_name_match = re.match(r"\"([^\"]+)\"\.\"([^\"]+)\"", cleaned_key)
            if table_name_match:
                table_name, column_name = table_name_match.groups()
                cleaned_alias_map[(table_name, column_name)] = value
            else:
                # If no table and column found, use cleaned_key as is
                cleaned_alias_map[cleaned_key] = value
        else:
            # If the key is already a tuple (table_name, column_name), clean the column name
            table_name, column_name = key
            cleaned_column_name = clean_column_name(column_name)
            cleaned_alias_map[(table_name, cleaned_column_name)] = value
    return cleaned_alias_map


def add_grain_from_table_or_cte(
    table_name, selected_columns, alias_map, tables_and_columns, cte_grain_map
):
    """Add grain columns from a table or CTE based on the selected columns and existing grain."""
    new_grain_columns = set()

    # Preprocess alias_map to clean its keys
    cleaned_alias_map = preprocess_alias_map(alias_map)

    if table_name in cte_grain_map:
        # Handle CTE as a table, treating cte_grain as primary keys
        cte_grain = cte_grain_map[table_name]
        primary_keys_in_grain = set()

        for col in cte_grain:
            # Clean the CTE grain column name and use it to look up the alias in the cleaned alias map
            clean_col = clean_column_name(col)
            alias = cleaned_alias_map.get((table_name, clean_col), clean_col)

            if alias in selected_columns:
                primary_keys_in_grain.add(alias)
            else:
                return None  # Return None if not all CTE grain columns are in selected columns

        if not primary_keys_in_grain or len(primary_keys_in_grain) != len(cte_grain):
            return None  # Ensure the grain is complete

        new_grain_columns.update(primary_keys_in_grain)

    else:
        # Handle regular table
        primary_keys = get_primary_keys(table_name, tables_and_columns)
        primary_keys_in_grain = set()

        for pk in primary_keys:
            # Clean the primary key and use it to look up the alias in the cleaned alias map
            clean_pk = clean_column_name(pk)
            alias = cleaned_alias_map.get((table_name, clean_pk), clean_pk)

            if alias in selected_columns:
                primary_keys_in_grain.add(alias)
            else:
                return (
                    None  # Return None if not all primary keys are in selected columns
                )

        if not primary_keys_in_grain or len(primary_keys_in_grain) != len(primary_keys):
            return None  # Ensure the grain is complete

        new_grain_columns.update(primary_keys_in_grain)

    return new_grain_columns


def process_from_and_joins(
    node,
    selected_columns,
    alias_map,
    tables_and_columns,
    cte_grain_map,
    reverse_table_alias_map,
):
    grain_columns = set()

    from_table_expr = node.args.get("from_")
    joins = node.args.get("joins", [])
    from_table = None

    if from_table_expr:
        from_table = from_table_expr.find(exp.Table)
        if not from_table:
            return None

    # Process joins if they exist
    if joins:
        left_primary_keys = {}
        left_table_name = from_table.name if from_table else None

        # Populate the left_primary_keys dictionary
        if left_table_name:
            left_primary_keys[left_table_name] = cte_grain_map.get(
                left_table_name
            ) or get_primary_keys(left_table_name, tables_and_columns)

        for join in joins:
            right_table = join.this.find(exp.Table)
            if right_table:
                join_condition = join.args.get("on")
                join_kind = join.args.get("kind")
                join_side = join.args.get("side")
                if join_kind == "CROSS":
                    join_type = "CROSS"
                elif join_side:
                    join_type = join_side
                else:
                    join_type = "INNER"
                join_grain = process_join(
                    left_primary_keys,
                    right_table.name,
                    join_condition,
                    selected_columns,
                    alias_map,
                    tables_and_columns,
                    cte_grain_map,
                    reverse_table_alias_map,
                    join_type,
                )
                if join_grain is None:
                    return None  # Early return if grain is incomplete
                grain_columns.update(join_grain)
                left_table_name = (
                    right_table.name
                )  # Update left table for the next join
                left_primary_keys[left_table_name] = cte_grain_map.get(
                    left_table_name
                ) or get_primary_keys(left_table_name, tables_and_columns)
    else:
        # If there are no joins, process the from table directly
        if from_table:
            from_grain = add_grain_from_table_or_cte(
                from_table.name,
                selected_columns,
                alias_map,
                tables_and_columns,
                cte_grain_map,
            )
            if from_grain is None:
                return None  # Early return if grain is incomplete
            grain_columns.update(from_grain)

    return grain_columns


def check_primary_keys_in_columns(
    columns_with_table,
    alias_map,
    cte_grain_map,
    left_primary_keys,
    right_primary_keys,
    right_table_name,
    reverse_table_alias_map,
):
    # Flatten columns_with_table to a set of tuples (table_alias, column_name)
    columns_set = set(columns_with_table)

    def check_keys_in_columns(primary_keys, table_name):
        """Helper function to check if all primary keys are present in columns_with_table."""
        if table_name in cte_grain_map:
            # If the table has a CTE grain, use it to check the keys
            table_grain = cte_grain_map[table_name]
        else:
            # Otherwise, use the primary keys directly
            table_grain = primary_keys

        # For each primary key, check if it's in columns_with_table either directly or via alias_map
        for pk in table_grain:
            # Check for (table_name, pk) or (alias, pk)
            original = (table_name, pk)
            alias = (table_name, alias_map.get(original))
            table_alias_original = (reverse_table_alias_map.get(table_name), pk)
            table_alias_alias = (
                reverse_table_alias_map.get(table_name),
                alias_map.get(original),
            )

            if (
                original not in columns_set
                and alias not in columns_set
                and table_alias_original not in columns_set
                and table_alias_alias not in columns_set
            ):
                return False
        return True

    # Check left primary keys
    left_result = True
    for table_name, primary_keys in left_primary_keys.items():
        if not check_keys_in_columns(primary_keys, table_name):
            left_result = False
            break

    # Check right primary keys
    right_result = check_keys_in_columns(right_primary_keys, right_table_name)

    return left_result, right_result


def process_join(
    left_primary_keys,
    right_table_name,
    join_condition,
    selected_columns,
    alias_map,
    tables_and_columns,
    cte_grain_map,
    tables_alias_map,
    join_type="INNER",
):
    grain_columns = set()

    def add_grain_for_table(table_name):
        """Helper function to add grain for a specific table"""
        table_grain = add_grain_from_table_or_cte(
            table_name, selected_columns, alias_map, tables_and_columns, cte_grain_map
        )
        if table_grain is not None:
            grain_columns.update(table_grain)
        return table_grain

    def handle_cross_join():
        """Handle CROSS JOIN by adding grains from both tables"""
        for left_table_name, pk_list in left_primary_keys.items():
            add_grain_for_table(left_table_name)  # Add grain for the left table

        add_grain_for_table(right_table_name)  # Add grain for the right table

    def handle_inner_join(left_keys_in_condition, right_keys_in_condition):
        """Handle INNER JOIN by checking if keys from both tables are in the join condition"""

        if right_keys_in_condition and left_keys_in_condition:
            # Attempt to add grain for both tables
            add_grain_for_table(right_table_name)
            add_grain_for_table(next(iter(left_primary_keys)))
        else:
            if not left_keys_in_condition:
                for left_table_name, _ in left_primary_keys.items():
                    add_grain_for_table(left_table_name)
            if not right_keys_in_condition:
                add_grain_for_table(right_table_name)

    def handle_left_join(right_keys_in_condition):
        """Handle LEFT JOIN by checking and adding appropriate grains"""
        if not right_keys_in_condition:
            grain_columns.update(
                right_primary_keys
            )  # Add right table keys if they are in the join condition
        add_grain_for_table(next(iter(left_primary_keys)))

    def handle_right_join(left_keys_in_condition):
        """Handle RIGHT JOIN by checking and adding appropriate grains"""
        if not left_keys_in_condition:
            grain_columns.update(
                next(iter(left_primary_keys))
            )  # Add left table keys if they are in the join condition
        add_grain_for_table(right_table_name)

    # Main logic to handle the different join types
    if join_type == "CROSS":
        handle_cross_join()
    elif join_condition and isinstance(join_condition, exp.Binary):
        # Extract all columns with their table aliases
        columns_with_table = extract_columns(join_condition)

        # Find the aliases for the left and right tables based on their columns
        right_primary_keys = (
            cte_grain_map.get(right_table_name)
            if (right_table_name in cte_grain_map)
            else get_primary_keys(right_table_name, tables_and_columns)
        )
        left_primary_keys_in_join_conditions, right_primary_keys_in_join_conditions = (
            check_primary_keys_in_columns(
                columns_with_table,
                alias_map,
                cte_grain_map,
                left_primary_keys,
                right_primary_keys,
                right_table_name,
                tables_alias_map,
            )
        )
        if join_type == "INNER":
            handle_inner_join(
                left_keys_in_condition=left_primary_keys_in_join_conditions,
                right_keys_in_condition=right_primary_keys_in_join_conditions,
            )
        elif join_type == "LEFT":
            handle_left_join(
                right_keys_in_condition=right_primary_keys_in_join_conditions
            )
        elif join_type == "RIGHT":
            handle_right_join(
                left_keys_in_condition=left_primary_keys_in_join_conditions
            )

    return grain_columns if grain_columns else None


def find_grain(node, tables_and_columns, selected_columns, cte_grain_map=None):
    grain_columns = set()

    if isinstance(node, exp.Select):
        # Step 1: Directly check if all selected columns are aggregates or literals
        select_expressions = node.args.get("expressions", [])
        if all(
            isinstance(expr.this, (exp.AggFunc, exp.Literal))
            for expr in select_expressions
        ):
            return [0]

        from_expr = node.args.get("from_")
        join_exprs = node.args.get("joins", [])

        # Step 2: Create alias map from SELECT expressions, FROM, and JOIN clauses
        alias_map, reverse_alias_map, tables_alias_map, reverse_table_alias_map = (
            map_aliases(select_expressions, from_expr, join_exprs)
        )

        # Step 3: Process CTEs before anything else
        if "with_" in node.args:
            process_with_clause(node, tables_and_columns, cte_grain_map)

        # Step 5: Process GROUP BY clause based on aliases
        group_by_columns = process_group_by(
            node,
            tables_and_columns,
            cte_grain_map,
            tables_alias_map,
            reverse_table_alias_map,
        )
        if group_by_columns:
            grain_columns.update(group_by_columns)
            return sorted(grain_columns)

        if node.args.get("distinct"):
            distinct_columns = process_distinct(
                node,
                tables_and_columns,
                cte_grain_map,
                tables_alias_map,
                reverse_table_alias_map,
            )
            if distinct_columns:
                return sorted(distinct_columns)
            return sorted(grain_columns) or sorted(selected_columns)

        # Step 6: Process FROM and JOIN clauses (focused on alias-level grains)
        from_and_join_grain = process_from_and_joins(
            node,
            selected_columns,
            alias_map,
            tables_and_columns,
            cte_grain_map,
            reverse_table_alias_map,
        )

        if from_and_join_grain is None:
            return None  # Early return if grain is incomplete
        grain_columns.update(from_and_join_grain)

        # Filter grain columns to only include those in selected columns
        grain_columns = grain_columns.intersection(selected_columns)

        # Return the grain columns
        return sorted(grain_columns) if grain_columns else None

    elif isinstance(node, exp.Subquery):
        expression = node.args.get("this")
        column_aliases = extract_column_aliases(expression)
        return find_grain(expression, tables_and_columns, column_aliases, cte_grain_map)

    elif isinstance(node, exp.Union):
        grains = []
        current_node = node

        # Process each part of the UNION
        while current_node:
            selected_columns = extract_column_aliases(current_node.args.get("this"))
            left_grain = find_grain(
                current_node.args.get("this"),
                tables_and_columns,
                selected_columns,
                cte_grain_map,
            )
            if left_grain is None:
                return None
            grains.append(set(left_grain))
            if not current_node.args.get("union"):
                break
            current_node = current_node.args.get("union")

        # Intersect the grains from each part of the UNION
        final_grain = set.intersection(*grains)
        return sorted(final_grain) if final_grain else None

    elif isinstance(node, exp.Table) and node.this in cte_grain_map:
        # Handle references to CTEs
        cte_grain = cte_grain_map[node.this]
        # Convert cte_grain to a set and intersect with selected columns
        if cte_grain != 0:
            return (
                sorted(set(cte_grain).intersection(selected_columns))
                if selected_columns
                else cte_grain
            )

    return None


def get_primary_keys(table_name, tables_and_columns):
    for table in tables_and_columns:
        if table["table_name"].split(".")[-1] == table_name:
            return table.get("primary_keys", [])
    return []


def extract_column_aliases(parsed_query: exp.Query):
    """
    Extract all column aliases from the SELECT clause of the parsed query.
    """
    aliases = set()

    def _extract_aliases(expr):
        if isinstance(expr, exp.Alias):
            aliases.add(expr.alias)
        elif isinstance(expr, exp.Column):
            aliases.add(expr.name)
        elif hasattr(expr, "expressions"):
            for sub_expr in expr.expressions:
                _extract_aliases(sub_expr)
        elif isinstance(expr, exp.Func):
            for arg in expr.args.values():
                _extract_aliases(arg)
        elif isinstance(expr, (exp.Binary, exp.Case)):
            _extract_aliases(expr.this)
            for sub_expr in expr.expressions:
                _extract_aliases(sub_expr)

    select_expressions = parsed_query.selects
    for expr in select_expressions:
        _extract_aliases(expr)

    return aliases


def clean_column_name(column_name):
    """Remove functions like CAST, COALESCE from the column name."""
    # First, remove the CAST, COALESCE function wrapping
    cleaned_name = re.sub(
        r"(CAST|COALESCE)\s*\(\s*([^)]+?)\s*\)\s*(AS\s+\w+\s*)?",
        r"\2",
        column_name,
        flags=re.IGNORECASE,
    )
    # If the cleaned_name still contains another nested function, clean it recursively
    if cleaned_name != column_name:
        return clean_column_name(cleaned_name)
    return cleaned_name.strip()
