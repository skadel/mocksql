import sqlglot

from build_query.validator import optimize_query

# Schéma d’exemple
TABLES = {"x": {"a": "STRING", "b": "STRING", "v": "INT"}}


class TestOptimizeQuery:
    def test_simple_group_by_no_constant(self):
        """
        Aucun champ constant et GROUP BY nominatif :
        la requête ne doit pas être modifiée.
        """
        sql = "SELECT a AS c, b AS d FROM x GROUP BY c, d"
        expr = sqlglot.parse_one(sql, dialect="bigquery")
        op = optimize_query(expr, TABLES, dialect="bigquery", optimize=True)
        res = op.sql(dialect="bigquery")
        expected = (
            "SELECT `x`.`a` AS `c`, `x`.`b` AS `d` "
            "FROM `x` AS `x` "
            "GROUP BY `x`.`a`, `x`.`b`"
        )
        assert res == expected

    def test_positional_group_by_constant_removal(self):
        """
        Champ constant en position 2, GROUP BY positionnel (1,2) :
        on doit retirer la constante et n’avoir que GROUP BY 1.
        """
        sql = "SELECT a AS c, 'X' AS d, COUNT(v) AS cnt FROM x GROUP BY 1, 2"
        expr = sqlglot.parse_one(sql, dialect="bigquery")
        op = optimize_query(expr, TABLES, dialect="bigquery", optimize=True)
        res = op.sql(dialect="bigquery")
        expected = (
            "SELECT `x`.`a` AS `c`, 'X' AS `d`, COUNT(`x`.`v`) AS `cnt` "
            "FROM `x` AS `x` "
            "GROUP BY `x`.`a`"
        )
        assert res == expected

    def test_named_group_by_constant_removal(self):
        """
        Champ constant aliasé ‘d’ et GROUP BY nominatif (c, d) :
        on doit retirer ‘d’ de la projection et de la clause GROUP BY.
        """
        sql = "SELECT a AS c, 'X' AS d, SUM(v) AS s FROM x GROUP BY c, d"
        expr = sqlglot.parse_one(sql, dialect="bigquery")
        op = optimize_query(expr, TABLES, dialect="bigquery", optimize=True)
        res = op.sql(dialect="bigquery")
        expected = (
            "SELECT `x`.`a` AS `c`, 'X' AS `d`, SUM(`x`.`v`) AS `s` "
            "FROM `x` AS `x` "
            "GROUP BY `x`.`a`"
        )
        assert res == expected

    def test_mixed_positional_and_named_group_by(self):
        """
        Champ constant en position 2 et GROUP BY mixte (1,e) :
        on retire la position 2 et on conserve la référence nommée e.
        """
        sql = "SELECT a AS c, 'X' AS d, b AS e FROM x GROUP BY 1, e"
        expr = sqlglot.parse_one(sql, dialect="bigquery")
        op = optimize_query(expr, TABLES, dialect="bigquery", optimize=True)
        res = op.sql(dialect="bigquery")
        expected = (
            "SELECT `x`.`a` AS `c`, 'X' AS `d`, `x`.`b` AS `e` "
            "FROM `x` AS `x` "
            "GROUP BY `x`.`a`, `x`.`b`"
        )
        assert res == expected

    def test_nested_subquery_constant_removal(self):
        """
        Dans une sous-requête, champ constant et GROUP BY nominatif c,d :
        la sous-requête doit voir ‘d’ retiré, mais la requête externe reste valide.
        """
        sql = """
        SELECT y.c, y.e
        FROM (
          SELECT a AS c, 'X' AS d, b AS e
          FROM x
          GROUP BY c, d, e
        ) AS y
        """
        expr = sqlglot.parse_one(sql, dialect="bigquery")
        op = optimize_query(expr, TABLES, dialect="bigquery", optimize=True)
        res = op.sql(dialect="bigquery")
        expected = (
            "WITH`y`AS(SELECT`x`.`a`AS`c`,`x`.`b`AS`e`FROM`x`AS`x`GROUPBY`x`.`a`,`x`.`b`)"
            "SELECT`y`.`c`AS`c`,`y`.`e`AS`e`FROM`y`AS`y`"
        )
        assert "".join(res.split()) == "".join(expected.split())

    def test_cte_named_group_by_cast_constant(self):
        """
        CTE avec CAST constant aliasé, GROUP BY nominatif (c, d).
        La colonne constante doit être retirée du SELECT et du GROUP BY interne.
        """
        sql = """
        WITH y AS (
          SELECT a AS c, CAST('1234' AS INT64) AS d
          FROM x
          GROUP BY c, d
        )
        SELECT c
        FROM y
        GROUP BY c
        """
        expr = sqlglot.parse_one(sql, dialect="bigquery")
        op = optimize_query(expr, TABLES, dialect="bigquery", optimize=True)
        res = op.sql(dialect="bigquery")
        expected = (
            "WITH `y` AS ("
            "SELECT `x`.`a` AS `c` "
            "FROM `x` AS `x` "
            "GROUP BY `x`.`a`"
            ") "
            "SELECT `y`.`c` AS `c` "
            "FROM `y` AS `y` "
            "GROUP BY `y`.`c`"
        )
        assert "".join(res.split()) == "".join(
            expected.split()
        )  # CAST constant removed

    def test_subquery_constant_and_group_by(self):
        """
        Projection principale : un sous‐select constant et COUNT(0), GROUP BY 1.
        Le sous‐select constant (position 1) doit être supprimé ainsi que le GROUP BY.
        """
        sql = "SELECT (SELECT COUNT(0) FROM A) AS c, COUNT(0) AS cnt FROM X GROUP BY 1"
        expr = sqlglot.parse_one(sql, dialect="bigquery")
        op = optimize_query(expr, TABLES, dialect="bigquery", optimize=True)
        res = op.sql(dialect="bigquery")
        expected = "SELECT(SELECTCOUNT(0)AS`_col_0`FROM`a`AS`a`)AS`c`,COUNT(0)AS`cnt`FROM`x`AS`x`"
        assert "".join(res.split()) == "".join(expected.split())

    def test_cast_constant_in_main_select(self):
        """
        SELECT principal avec CAST constant en position 1 et un champ a positionnellement groupé.
        On enlève la constante et on réajuste GROUP BY 1 vers la colonne a.
        """
        sql = "SELECT CAST('1234' AS INT64) AS d, a FROM x GROUP BY 2"
        expr = sqlglot.parse_one(sql, dialect="bigquery")
        op = optimize_query(expr, TABLES, dialect="bigquery", optimize=True)
        res = op.sql(dialect="bigquery")
        expected = (
            "SELECT CAST('1234' AS INT64) AS`d`, `x`.`a` AS `a` "
            "FROM `x` AS `x` "
            "GROUP BY `x`.`a`"
        )
        assert "".join(res.split()) == "".join(expected.split())
