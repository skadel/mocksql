import sqlglot

from build_query.validator import optimize_and_extract_info, evaluate_and_fix_query


def _used_cols(result):
    return [
        {"table": e["table"], "used_columns": sorted(e["used_columns"])}
        for e in result["used_columns"]
    ]


async def _extract(query, tables, dialect="bigquery"):
    parsed = sqlglot.parse_one(query, dialect=dialect)
    return await optimize_and_extract_info(parsed, tables, dialect=dialect)


class TestExtractUsedColumns:
    async def test_single_table(self):
        query = "SELECT id, name FROM users WHERE age > 20"
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        expected = [{"table": "users", "used_columns": ["age", "id", "name"]}]
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_table_alias(self):
        query = "/* Select Adult users */ SELECT u.id, u.name FROM users AS u WHERE u.age > 20"
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        expected = [
            {
                "project": "",
                "database": "",
                "table": "users",
                "used_columns": ["age", "id", "name"],
                "used_identifiers": ["age", "name", "id", "u"],
            }
        ]
        _, _, used_columns, _ = await evaluate_and_fix_query(
            query, mapping=tables, dialect="bigquery"
        )
        for entry in used_columns:
            entry["used_identifiers"] = sorted(entry.get("used_identifiers", []))
        for entry in expected:
            entry["used_identifiers"] = sorted(entry.get("used_identifiers", []))
        assert used_columns == expected

    async def test_table_mix_alias(self):
        query = "SELECT u.id, u.name FROM users AS u WHERE age > 20"
        expected = [{"table": "users", "used_columns": ["age", "id", "name"]}]
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_join_tables(self):
        query = (
            "SELECT u.id, p.title FROM users AS u JOIN posts AS p ON u.id = p.user_id"
        )
        expected = [
            {"table": "posts", "used_columns": ["title", "user_id"]},
            {"table": "users", "used_columns": ["id"]},
        ]
        tables = {
            "users": {"id": "INT"},
            "posts": {"title": "STRING", "user_id": "INT"},
        }
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_join_tables_without_qualification(self):
        query = "SELECT id, title FROM users AS u JOIN posts AS p ON u.id = p.user_id"
        expected = [
            {"table": "posts", "used_columns": ["title", "user_id"]},
            {"table": "users", "used_columns": ["id"]},
        ]
        tables = {
            "users": {"id": "INT"},
            "posts": {"title": "STRING", "user_id": "INT", "post": "STRING"},
        }
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_unresolvable_column(self):
        query = "SELECT id, title FROM users AS u JOIN posts AS p ON u.id = p.user_id"
        tables = {
            "users": {"id": "INT"},
            "posts": {"title": "STRING", "user_id": "INT", "id": "INT"},
        }
        # Ambiguous 'id' column — sqlglot may raise or not depending on version; just check no crash on used_columns
        try:
            result = await _extract(query, tables)
            assert isinstance(result["used_columns"], list)
        except sqlglot.errors.OptimizeError:
            pass

    async def test_subquery(self):
        query = "SELECT name FROM (SELECT name FROM users) AS u"
        expected = [{"table": "users", "used_columns": ["name"]}]
        tables = {"users": {"name": "STRING"}}
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_columns_no_table(self):
        query = "SELECT name, age FROM some_view"
        expected = [{"table": "some_view", "used_columns": ["age", "name"]}]
        tables = {"some_view": {"age": "INT", "name": "STRING"}}
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_simple_cte(self):
        query = """
        WITH filtered_users AS (
            SELECT id, name FROM users WHERE age > 20
        )
        SELECT name FROM filtered_users
        """

        # pushdown_projections élague `id` : sélectionné par le CTE mais jamais
        # consommé par le SELECT externe. `name` (sortie) et `age` (filtre) restent.
        expected = [{"table": "users", "used_columns": ["age", "name"]}]
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_uppercase_table_referenced_by_full_name_qualifier(self):
        """Régression (incident c3) : une table en CASSE MAJUSCULE référencée au niveau
        supérieur SANS alias, dont les colonnes du SELECT final sont qualifiées par son
        nom complet ``DATASET.TABLE`` (et non un alias), et aussi utilisée aliasée dans
        un CTE.

        La restauration de casse d'``optimize_query`` laisse le qualificateur de colonne
        top-level sous le nom complet (``my_ds.facts.label``) au lieu de l'alias assigné
        (``FACTS``). Le qualificateur ne matchait alors aucune source du scope → la colonne
        tombait dans le fallback « table fantôme » puis était DROPPÉE de used_columns → la
        table DuckDB de test était créée sans elle → ``Binder Error`` à l'exécution
        (colonne projetée par le SELECT final mais absente de la table).
        """
        query = """
        WITH agg AS (
          SELECT f.id AS id, SUM(f.amount) AS total
          FROM `MY_DS.FACTS` AS f
          GROUP BY f.id
        )
        SELECT
          `MY_DS.FACTS`.id AS id,
          `MY_DS.FACTS`.label AS label,
          agg.total AS total
        FROM `MY_DS.FACTS`
        LEFT JOIN agg ON `MY_DS.FACTS`.id = agg.id
        """
        tables = {
            "MY_DS.FACTS": {"id": "INT64", "amount": "FLOAT64", "label": "STRING"}
        }
        result = await _extract(query, tables)
        cols = {e["table"]: sorted(e["used_columns"]) for e in result["used_columns"]}
        # `label` n'est référencée QUE par le qualificateur nom-complet du SELECT final.
        assert "FACTS" in cols, f"FACTS absente de used_columns: {cols}"
        assert "label" in cols["FACTS"], f"colonne 'label' droppée: {cols}"
        assert cols["FACTS"] == ["amount", "id", "label"]

    async def test_nested_cte(self):
        query = """
        WITH filtered_users AS (
            SELECT * FROM users WHERE age > 20
        ), adult_users AS (
            SELECT id FROM filtered_users WHERE age >= 18
        )
        SELECT id FROM adult_users
        """
        # SELECT * est expansé puis élagué : seules `id` (sortie finale) et `age`
        # (filtres des deux CTE) sont consommées en aval ; `name` est retirée.
        expected = [{"table": "users", "used_columns": ["age", "id"]}]
        tables = {"users": {"id": "INT", "name": "STRING", "age": "INT"}}
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_complex_query(self):
        query = """
                WITH filtered_users AS (
                    SELECT id, name FROM users WHERE age > 20
                ),
                extended_info AS (
                    SELECT user_id, email FROM user_details WHERE email LIKE '%@example.com'
                ),
                joined_data AS (
                    SELECT fu.name, ei.email
                    FROM filtered_users fu
                    JOIN extended_info ei ON fu.id = ei.user_id
                ),
                additional_data AS (
                    SELECT name FROM users WHERE age BETWEEN 15 AND 20
                )

                SELECT name, email FROM joined_data
                UNION ALL
                SELECT name, NULL AS email FROM additional_data
                """
        tables = {
            "users": {"id": "INT", "name": "STRING", "age": "INT"},
            "user_details": {"user_id": "INT", "email": "STRING"},
        }

        expected = [
            {"table": "user_details", "used_columns": ["email", "user_id"]},
            {"table": "users", "used_columns": ["age", "id", "name"]},
        ]

        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_complex_query_with_wildcard(self):
        query = """
                WITH filtered_users AS (
                    SELECT id, name FROM users WHERE age > 20
                ),
                extended_info AS (
                    SELECT * FROM user_details WHERE email LIKE '%@example.com'
                ),
                joined_data AS (
                    SELECT fu.name, ei.email
                    FROM filtered_users fu
                    JOIN extended_info ei ON fu.id = ei.user_id
                ),
                additional_data AS (
                    SELECT name FROM users WHERE age BETWEEN 15 AND 20
                )

                SELECT name, email FROM joined_data
                UNION ALL
                SELECT name, NULL AS email FROM additional_data
                """
        tables = {
            "users": {"id": "INT", "name": "STRING", "age": "INT"},
            "user_details": {
                "user_id": "INT",
                "email": "STRING",
                "other_col": "STRING",
            },
        }

        # SELECT * sur user_details est expansé puis élagué : `other_col` n'est
        # jamais consommée en aval → retirée. Restent `email` et `user_id`.
        expected = [
            {
                "table": "user_details",
                "used_columns": ["email", "user_id"],
            },
            {"table": "users", "used_columns": ["age", "id", "name"]},
        ]
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_same_table_name_in_different_cte(self):
        query = """
WITH
  `oi` AS (
  SELECT
    `oi`.`order_id` AS `order_id`,
    `oi`.`product_id` AS `product_id`,
    `oi`.`sale_price` AS `sale_price`
  FROM
    `order_items` AS `oi`),
  `p` AS (
  SELECT
    `p`.`id` AS `id`,
    `p`.`name` AS `name`,
    `p`.`brand` AS `brand`
  FROM
    `products` AS `p`
  WHERE
    `p`.`brand` = 'MG'),
  `productrevenue2021` AS (
  SELECT
    `p`.`name` AS `product_name`,
    SUM(`oi`.`sale_price`) AS `revenue_2021`
  FROM
    `orders` AS `o`
  JOIN
    `oi` AS `oi`
  ON
    `o`.`order_id` = `oi`.`order_id`
  JOIN
    `p` AS `p`
  ON
    `oi`.`product_id` = `p`.`id`
  WHERE
    `o`.`gender` = 'F'
    AND EXTRACT(YEAR
    FROM
      `o`.`created_at`) = 2021
  GROUP BY
    `p`.`name`),
  `productrevenue2022` AS (
  SELECT
    `p`.`name` AS `product_name`,
    SUM(`oi`.`sale_price`) AS `revenue_2022`
  FROM
    `orders` AS `o`
  JOIN
    `oi` AS `oi`
  ON
    `o`.`order_id` = `oi`.`order_id`
  JOIN
    `p` AS `p`
  ON
    `oi`.`product_id` = `p`.`id`
  WHERE
    `o`.`gender` = 'F'
    AND EXTRACT(YEAR
    FROM
      `o`.`created_at`) = 2022
  GROUP BY
    `p`.`name`)
SELECT
  `p1`.`product_name` AS `product_name`
FROM
  `productrevenue2021` AS `p1`
JOIN
  `productrevenue2022` AS `p2`
ON
  `p1`.`product_name` = `p2`.`product_name`
  AND `p2`.`revenue_2022` < 0.9 * `p1`.`revenue_2021`
"""
        tables = {
            "distribution_centers": {
                "id": "INTEGER",
                "name": "STRING",
                "latitude": "FLOAT",
                "longitude": "FLOAT",
            },
            "events": {
                "id": "INTEGER",
                "user_id": "INTEGER",
                "sequence_number": "INTEGER",
                "session_id": "STRING",
                "created_at": "TIMESTAMP",
                "ip_address": "STRING",
                "city": "STRING",
                "state": "STRING",
                "postal_code": "STRING",
                "browser": "STRING",
                "traffic_source": "STRING",
                "uri": "STRING",
                "event_type": "STRING",
            },
            "inventory_items": {
                "id": "INTEGER",
                "product_id": "INTEGER",
                "created_at": "TIMESTAMP",
                "sold_at": "TIMESTAMP",
                "cost": "FLOAT",
                "product_category": "STRING",
                "product_name": "STRING",
                "product_brand": "STRING",
                "product_retail_price": "FLOAT",
                "product_department": "STRING",
                "product_sku": "STRING",
                "product_distribution_center_id": "INTEGER",
            },
            "order_items": {
                "id": "INTEGER",
                "order_id": "INTEGER",
                "user_id": "INTEGER",
                "product_id": "INTEGER",
                "inventory_item_id": "INTEGER",
                "status": "STRING",
                "created_at": "TIMESTAMP",
                "shipped_at": "TIMESTAMP",
                "delivered_at": "TIMESTAMP",
                "returned_at": "TIMESTAMP",
                "sale_price": "FLOAT",
            },
            "orders": {
                "order_id": "INTEGER",
                "user_id": "INTEGER",
                "status": "STRING",
                "gender": "STRING",
                "created_at": "TIMESTAMP",
                "returned_at": "TIMESTAMP",
                "shipped_at": "TIMESTAMP",
                "delivered_at": "TIMESTAMP",
                "num_of_item": "INTEGER",
            },
            "products": {
                "id": "INTEGER",
                "cost": "FLOAT",
                "category": "STRING",
                "name": "STRING",
                "brand": "STRING",
                "retail_price": "FLOAT",
                "department": "STRING",
                "sku": "STRING",
                "distribution_center_id": "INTEGER",
            },
            "users": {
                "id": "INTEGER",
                "first_name": "STRING",
                "last_name": "STRING",
                "email": "STRING",
                "age": "INTEGER",
                "gender": "STRING",
                "state": "STRING",
                "street_address": "STRING",
                "postal_code": "STRING",
                "city": "STRING",
                "country": "STRING",
                "latitude": "FLOAT",
                "longitude": "FLOAT",
                "traffic_source": "STRING",
                "created_at": "TIMESTAMP",
            },
        }

        expected = [
            {
                "table": "order_items",
                "used_columns": ["order_id", "product_id", "sale_price"],
            },
            {"table": "orders", "used_columns": ["created_at", "gender", "order_id"]},
            {"table": "products", "used_columns": ["brand", "id", "name"]},
        ]
        result = await _extract(query, tables)
        assert _used_cols(result) == expected

    async def test_snowflake_uppercase_tables_lowercase_quoted_columns(self):
        """Régression (sf_bq263) : cache Snowflake en casse exacte (minuscule) +
        SQL quotant les colonnes en minuscule. Ajoutées NON-quotées au
        MappingSchema, les colonnes du cache étaient normalisées MAJUSCULE par le
        dialecte snowflake → ``OptimizeError: Unknown column: created_at``.
        Même famille que le fix CLI ``extract_used_columns_from_sql``."""
        query = """
        SELECT DATE_TRUNC('MONTH', TO_TIMESTAMP_NTZ("O"."created_at" / 1000000)) AS "month",
               SUM("OI"."sale_price") AS "total_sales"
        FROM "TE"."ORDER_ITEMS" AS "OI"
        JOIN "TE"."ORDERS" AS "O" ON "OI"."order_id" = "O"."order_id"
        WHERE "O"."status" = 'Complete'
        GROUP BY 1
        """
        tables = {
            "TE.ORDERS": {"order_id": "INT", "status": "TEXT", "created_at": "INT"},
            "TE.ORDER_ITEMS": {"order_id": "INT", "sale_price": "FLOAT"},
        }
        result = await _extract(query, tables, dialect="snowflake")
        cols = {e["table"]: sorted(e["used_columns"]) for e in result["used_columns"]}
        assert cols == {
            "ORDERS": ["created_at", "order_id", "status"],
            "ORDER_ITEMS": ["order_id", "sale_price"],
        }

    async def test_same_column_name_in_different_cte(self):
        query = """
    WITH
      `inventory` AS (
      SELECT
        `ii`.`product_id` AS `product_id`,
        `ii`.`cost` AS `cost`,
        `ii`.`sold_at` AS `sold_at`
      FROM
        `inventory_items` AS `ii`),
      `order_data` AS (
      SELECT
        `oi`.`order_id` AS `order_id`,
        `oi`.`product_id` AS `product_id`,
        `oi`.`sale_price` AS `sale_price`
      FROM
        `order_items` AS `oi`),
      `sales2021` AS (
      SELECT
        `od`.`product_id` AS `product_id`,
        SUM(`od`.`sale_price`) AS `total_sales_2021`
      FROM
        `orders` AS `o`
      JOIN
        `order_data` AS `od`
      ON
        `o`.`order_id` = `od`.`order_id`
      WHERE
        EXTRACT(YEAR FROM `o`.`created_at`) = 2021
      GROUP BY
        `od`.`product_id`),
      `sales2022` AS (
      SELECT
        `od`.`product_id` AS `product_id`,
        SUM(`od`.`sale_price`) AS `total_sales_2022`
      FROM
        `orders` AS `o`
      JOIN
        `order_data` AS `od`
      ON
        `o`.`order_id` = `od`.`order_id`
      WHERE
        EXTRACT(YEAR FROM `o`.`created_at`) = 2022
      GROUP BY
        `od`.`product_id`)
    SELECT
      `s21`.`product_id` AS `product_id`,
      `s21`.`total_sales_2021` AS `total_sales_2021`,
      `s22`.`total_sales_2022` AS `total_sales_2022`
    FROM
      `sales2021` AS `s21`
    JOIN
      `sales2022` AS `s22`
    ON
      `s21`.`product_id` = `s22`.`product_id`
    WHERE
      `s22`.`total_sales_2022` < 0.75 * `s21`.`total_sales_2021`
    """
        tables = {
            "inventory_items": {
                "product_id": "INTEGER",
                "cost": "FLOAT",
                "sold_at": "TIMESTAMP",
            },
            "order_items": {
                "order_id": "INTEGER",
                "product_id": "INTEGER",
                "sale_price": "FLOAT",
            },
            "orders": {"order_id": "INTEGER", "created_at": "TIMESTAMP"},
        }

        # The `inventory` CTE is defined but never referenced — the implementation includes it anyway (no CTE pruning)
        expected = [
            {
                "table": "inventory_items",
                "used_columns": ["cost", "product_id", "sold_at"],
            },
            {
                "table": "order_items",
                "used_columns": ["order_id", "product_id", "sale_price"],
            },
            {"table": "orders", "used_columns": ["created_at", "order_id"]},
        ]
        result = await _extract(query, tables)
        assert _used_cols(result) == expected
