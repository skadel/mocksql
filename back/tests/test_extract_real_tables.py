from utils.sql_code import extract_real_table_refs


def full_name(t) -> str:
    parts = [t.catalog, t.db, t.name]
    return ".".join(p.lower() for p in parts if p)


def names(sql: str, dialect: str = "bigquery") -> set[str]:
    return {full_name(t) for t in extract_real_table_refs(sql, dialect)}


class TestExtractRealTableRefs:
    # -----------------------
    # BASIC
    # -----------------------

    def test_simple_query(self):
        assert names("SELECT * FROM orders") == {"orders"}

    def test_join(self):
        assert names(
            "SELECT * FROM orders o JOIN customers c ON o.customer_id = c.id"
        ) == {"orders", "customers"}

    def test_no_tables(self):
        assert names("SELECT 1 + 1") == set()

    # -----------------------
    # BIGQUERY QUALIFICATION
    # -----------------------

    def test_dataset_table(self):
        assert names("SELECT * FROM dataset.orders") == {"dataset.orders"}

    def test_project_dataset_table(self):
        assert names("SELECT * FROM project.dataset.orders") == {
            "project.dataset.orders"
        }

    def test_backticks(self):
        assert names("SELECT * FROM `project.dataset.orders`") == {
            "project.dataset.orders"
        }

    # -----------------------
    # CTE BEHAVIOR
    # -----------------------

    def test_cte_excluded(self):
        sql = "WITH cte AS (SELECT * FROM orders) SELECT * FROM cte"
        assert names(sql) == {"orders"}

    def test_cte_shadowing_real_table(self):
        sql = """
        WITH users AS (
            SELECT * FROM users WHERE active = TRUE
        )
        SELECT * FROM users
        """
        assert names(sql) == {"users"}

    def test_cte_chain(self):
        sql = """
        WITH
          a AS (SELECT * FROM t1),
          b AS (SELECT * FROM a JOIN t2 ON a.id = t2.id)
        SELECT * FROM b
        """
        assert names(sql) == {"t1", "t2"}

    def test_multiple_ctes_same_table(self):
        sql = """
        WITH
          c1 AS (SELECT * FROM orders),
          c2 AS (SELECT * FROM orders)
        SELECT * FROM c1 UNION ALL SELECT * FROM c2
        """
        assert names(sql) == {"orders"}

    # -----------------------
    # SUBQUERIES
    # -----------------------

    def test_subquery(self):
        sql = "SELECT * FROM (SELECT * FROM orders)"
        assert names(sql) == {"orders"}

    def test_nested_subqueries(self):
        sql = """
        SELECT * FROM (
            SELECT * FROM (
                SELECT * FROM orders
            )
        )
        """
        assert names(sql) == {"orders"}

    # -----------------------
    # 🔥 TRICKY / EDGE CASES
    # -----------------------

    def test_unnest_not_a_table(self):
        sql = """
        SELECT *
        FROM orders,
        UNNEST(items) AS item
        """
        assert names(sql) == {"orders"}

    def test_array_subquery(self):
        sql = """
        SELECT ARRAY(
            SELECT id FROM orders
        )
        """
        assert names(sql) == {"orders"}

    def test_struct_subquery(self):
        sql = """
        SELECT AS STRUCT (
            SELECT id FROM orders
        )
        """
        assert names(sql) == {"orders"}

    def test_with_offset(self):
        sql = """
        SELECT *
        FROM orders, UNNEST(items) WITH OFFSET AS pos
        """
        assert names(sql) == {"orders"}

    def test_duplicate_tables(self):
        sql = """
        SELECT *
        FROM orders o1
        JOIN orders o2 ON o1.id = o2.parent_id
        """
        assert names(sql) == {"orders"}

    def test_join_with_subquery_and_table(self):
        sql = """
        SELECT *
        FROM (SELECT * FROM orders) o
        JOIN customers c ON o.customer_id = c.id
        """
        assert names(sql) == {"orders", "customers"}

    def test_cte_and_real_table_same_name_different_dataset(self):
        sql = """
        WITH orders AS (
            SELECT * FROM project.dataset.orders
        )
        SELECT * FROM orders
        """
        assert names(sql) == {"project.dataset.orders"}

    def test_wildcard_table(self):
        # BigQuery: table wildcard (orders_*)
        sql = "SELECT * FROM `project.dataset.orders_*`"
        assert names(sql) == {"project.dataset.orders_*"}

    def test_function_not_table(self):
        sql = "SELECT COUNT(*) FROM UNNEST([1,2,3])"
        assert names(sql) == set()

    def test_table_function_ignored(self):
        sql = "SELECT * FROM my_function()"
        assert names(sql) == set()

    def test_lateral_like_behavior(self):
        sql = """
        SELECT *
        FROM orders o,
        UNNEST(o.items) item
        """
        assert names(sql) == {"orders"}

    def test_complex_mix(self):
        sql = """
        WITH a AS (
            SELECT * FROM project.dataset.orders
        ),
        b AS (
            SELECT *
            FROM a
            JOIN project.dataset.customers c
            ON a.customer_id = c.id
        )
        SELECT *
        FROM b, UNNEST(b.items)
        """
        assert names(sql) == {
            "project.dataset.orders",
            "project.dataset.customers",
        }

    def test_cte_unpivot_sqlglot_bug(self):
        # Régression : SQLglot ne propage pas les CTEs dans le scope UNPIVOT
        # quand la CTE intermédiaire est définie dans la même WITH clause.
        # scope.ctes est vide pour le scope UNPIVOT → nvx_pdv était retourné
        # comme une vraie table avec l'ancien algorithme (scope uniquement).
        sql = """
        WITH
        cte1 AS (
            SELECT code FROM dataset.real_table
        ),
        nvx_pdv AS (
            SELECT * FROM cte1
        ),
        final AS (
            SELECT * FROM nvx_pdv
            UNPIVOT(valeur FOR indicateur IN (code))
        )
        SELECT * FROM final
        """
        assert names(sql) == {"dataset.real_table"}

    def test_pivot_on_chained_cte_sqlglot_bug(self):
        # Régression : même bug que UNPIVOT — le scope du CTE contenant PIVOT
        # a scope.ctes vide, donc la CTE intermédiaire `src` était retournée
        # comme une vraie table.
        sql = """
        WITH
        src AS (
            SELECT cat, val FROM ds.t
        ),
        pivoted AS (
            SELECT * FROM src
            PIVOT(SUM(val) FOR cat IN ('a', 'b'))
        )
        SELECT * FROM pivoted
        """
        assert names(sql) == {"ds.t"}

    def test_double_unpivot_chain_sqlglot_bug(self):
        # Régression : quand plusieurs CTEs enchaînés contiennent chacun un UNPIVOT,
        # tous leurs scopes ont scope.ctes vide. L'ancien algo retournait `a` et `b`
        # comme vraies tables au lieu de les filtrer.
        sql = """
        WITH
        a AS (
            SELECT x, y FROM ds.t
        ),
        b AS (
            SELECT * FROM a
            UNPIVOT(v FOR k IN (x, y))
        ),
        c AS (
            SELECT * FROM b
            UNPIVOT(v2 FOR k2 IN (v))
        )
        SELECT * FROM c
        """
        assert names(sql) == {"ds.t"}

    def test_cte_with_aggregations_and_unpivot(self):
        sql = """
        WITH agg AS (
            SELECT
                customer_id,
                COUNT(0) AS nb_ope,
                SUM(amount) AS mt_ope
            FROM project.dataset.orders
            GROUP BY customer_id
        )
        SELECT *
        FROM agg
        UNPIVOT (
            value FOR metric IN (nb_ope, mt_ope)
        )
        """
        assert names(sql) == {"project.dataset.orders"}

    def test_unqualified_real_table_same_name_as_cte(self):
        # Une CTE s'appelle 'orders' et masque une vraie table 'orders' (sans dataset).
        # On s'assure que la vraie table 'orders' à l'intérieur de la CTE n'est pas ignorée.
        sql = """
        WITH orders AS (
            SELECT * FROM orders WHERE active = TRUE
        )
        SELECT * FROM orders
        """
        assert names(sql) == {"orders"}

        # Cas 2 : Une CTE s'appelle 'users', mais on fait un JOIN avec une vraie table 'users'
        sql2 = """
        WITH users AS (SELECT 1 AS id)
        SELECT * FROM users cte_ref
        JOIN ds.users real_table ON cte_ref.id = real_table.id
        """
        assert names(sql2) == {"ds.users"}

    def test_time_travel(self):
        # BigQuery Time Travel : s'assurer que le modificateur n'interfère pas
        sql = """
        SELECT * 
        FROM project.dataset.orders FOR SYSTEM_TIME AS OF TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 HOUR)
        """
        assert names(sql) == {"project.dataset.orders"}

    def test_pivot(self):
        # Comme UNPIVOT, PIVOT crée un noeud AST complexe
        sql = """
        WITH cte AS (SELECT * FROM project.dataset.orders)
        SELECT * FROM cte
        PIVOT(SUM(amount) FOR status IN ('PENDING', 'SHIPPED'))
        """
        assert names(sql) == {"project.dataset.orders"}

    def test_project_with_hyphens(self):
        # Les noms de projets GCP contiennent souvent des tirets
        sql = "SELECT * FROM `my-super-project.my_dataset.my_table`"
        assert names(sql) == {"my-super-project.my_dataset.my_table"}
