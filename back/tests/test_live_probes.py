"""Phase 1.5b — sondes `inspect --live` : décomposition en préfixes + waterfall.

Vérifie la construction PURE des requêtes COUNT par frontière de JOIN (sur les vraies
tables, dialecte source) et l'annotation du waterfall (ratio de fan-out, côté non-unique).
Aucune I/O — l'exécution gatée est testée ailleurs (inspect_live).
"""

from build_query.live_probes import build_live_probes, classify_waterfall


def _cte(name, code):
    return {"name": name, "code": code, "dependencies": [], "sources": []}


FLAT = (
    "SELECT COUNT(DISTINCT O.order_id) AS c "
    "FROM order_items AS OI "
    "JOIN orders AS O ON OI.order_id = O.order_id "
    "JOIN products AS P ON OI.product_id = P.id "
    "WHERE O.status = 'Complete'"
)


# ── build_live_probes : scan + une sonde par join ────────────────────────────


def test_scan_then_one_probe_per_join():
    result = build_live_probes([_cte("final_query", FLAT)], "bigquery")
    assert len(result) == 1
    probes = result[0]["probes"]
    # FLAT porte un COUNT(DISTINCT …) → sonde pré-agrégat en dernière frontière.
    assert [p["boundary"] for p in probes] == ["scan", "join", "join", "pre_agg"]


def test_count_sql_accumulates_prefix():
    probes = build_live_probes([_cte("final_query", FLAT)], "bigquery")[0]["probes"]
    scan, j0, j1 = probes[:3]
    assert "COUNT(*)" in scan["count_sql"] and "order_items" in scan["count_sql"]
    assert "JOIN orders" in j0["count_sql"]
    assert "JOIN orders" in j1["count_sql"] and "JOIN products" in j1["count_sql"]
    # Le scan n'a pas encore le premier join.
    assert "JOIN orders" not in scan["count_sql"]


def test_right_key_and_distinct_probe():
    probes = build_live_probes([_cte("final_query", FLAT)], "bigquery")[0]["probes"]
    j0 = probes[1]  # ⋈ orders ON OI.order_id = O.order_id → clé droite O.order_id
    assert j0["right_key"] == "O.order_id"
    assert "COUNT(DISTINCT" in j0["right_distinct_sql"]
    assert "FROM orders AS O" in j0["right_distinct_sql"]


def test_no_joins_returns_empty():
    assert build_live_probes([_cte("final_query", "SELECT 1")], "bigquery") == []


def test_unparseable_cte_is_skipped():
    assert build_live_probes([_cte("final_query", "NOT SQL ((")], "bigquery") == []


# ── tiered vs full : sélection des CTE cibles ────────────────────────────────

CTE_A = "SELECT x.k AS k FROM t1 AS x JOIN t2 AS y ON x.k = y.k"
CTE_FINAL = "SELECT * FROM a JOIN t3 AS z ON a.k = z.k"


def test_tiered_probes_only_last_joined_cte():
    ctes = [_cte("a", CTE_A), _cte("final_query", CTE_FINAL)]
    tiered = build_live_probes(ctes, "bigquery", full=False)
    assert len(tiered) == 1
    assert tiered[0]["cte"] == "final_query"


def test_full_probes_every_joined_cte():
    ctes = [_cte("a", CTE_A), _cte("final_query", CTE_FINAL)]
    full = build_live_probes(ctes, "bigquery", full=True)
    assert {t["cte"] for t in full} == {"a", "final_query"}


def test_preceding_cte_included_as_with_prefix():
    # La sonde de final_query doit embarquer la CTE `a` en WITH (sinon `FROM a` invalide).
    ctes = [_cte("a", CTE_A), _cte("final_query", CTE_FINAL)]
    probes = build_live_probes(ctes, "bigquery", full=False)[0]["probes"]
    sql = probes[0]["count_sql"]
    assert sql.strip().upper().startswith("WITH")
    # `a` est embarquée en CTE (quoting selon dialecte : backticks en bigquery).
    assert "a` AS (" in sql or '"a" AS (' in sql or "a AS (" in sql


# ── classify_waterfall : ratios + côté non-unique ────────────────────────────


def test_classify_flags_fanout_ratio():
    probes = [
        {"label": "order_items", "rows": 90},
        {"label": "⋈ orders", "rows": 340, "right_rows": 100, "right_distinct": 100},
        {"label": "⋈ products", "rows": 340, "right_rows": 50, "right_distinct": 50},
    ]
    w = classify_waterfall(probes)
    # scan : pas de ratio (pas de précédent).
    assert "fanout_ratio" not in w[0]
    # ⋈ orders : 90 → 340 = ×3,78, fan_out.
    assert w[1]["fanout_ratio"] == 3.78
    assert w[1]["verdict"] == "fan_out"
    # ⋈ products : 340 → 340 = preserves.
    assert w[2]["verdict"] == "preserves"


def test_classify_marks_right_non_unique():
    probes = [
        {"label": "orders", "rows": 90},
        # order_items a 340 lignes mais 90 clés distinctes → côté droit non-unique.
        {
            "label": "⋈ order_items",
            "rows": 340,
            "right_rows": 340,
            "right_distinct": 90,
        },
    ]
    w = classify_waterfall(probes)
    assert w[1]["right_non_unique"] is True


def test_classify_empty_join():
    w = classify_waterfall([{"rows": 90}, {"rows": 0}])
    assert w[1]["verdict"] == "empty"


# ── sonde pré-agrégat : COUNT(*) vs COUNT(DISTINCT clé) à l'entrée du GROUP BY ─
# C'est la sonde cheap n°1 du plan (signature sf_bq263 : 340 lignes / 90 commandes).


def test_pre_agg_probe_added_when_count_distinct_present():
    probes = build_live_probes([_cte("final_query", FLAT)], "bigquery")[0]["probes"]
    assert probes[-1]["boundary"] == "pre_agg"
    pa = probes[-1]
    # Une seule requête qui mesure n ET d sur le préfixe complet, WHERE compris.
    assert "COUNT(*)" in pa["count_sql"] and "COUNT(DISTINCT" in pa["count_sql"]
    assert "WHERE" in pa["count_sql"].upper()
    assert "order_id" in pa["distinct_key"]


def test_no_pre_agg_probe_without_count_distinct():
    sql = "SELECT SUM(x.v) AS s FROM t1 AS x JOIN t2 AS y ON x.k = y.k GROUP BY x.k"
    probes = build_live_probes([_cte("final_query", sql)], "bigquery")[0]["probes"]
    assert all(p["boundary"] != "pre_agg" for p in probes)


def test_pre_agg_falls_back_to_plain_count_column():
    # Le SQL BUGUÉ (COUNT sans DISTINCT — le cas même où on lance inspect --live) n'a
    # pas de COUNT(DISTINCT) : l'axe est alors l'argument du COUNT(col) nu. Mesurer
    # n vs COUNT(DISTINCT col) révèle exactement le sur-comptage.
    buggy = (
        "SELECT COUNT(O.order_id) AS c FROM order_items AS OI "
        "JOIN orders AS O ON OI.order_id = O.order_id GROUP BY O.status"
    )
    probes = build_live_probes([_cte("final_query", buggy)], "bigquery")[0]["probes"]
    pa = probes[-1]
    assert pa["boundary"] == "pre_agg"
    assert "order_id" in pa["distinct_key"]
    # COUNT(*) n'est pas un axe : pas de sonde pré-agrégat sur COUNT(*) seul.
    star = "SELECT COUNT(*) AS c FROM t1 AS x JOIN t2 AS y ON x.k = y.k GROUP BY x.k"
    probes2 = build_live_probes([_cte("final_query", star)], "bigquery")[0]["probes"]
    assert all(p["boundary"] != "pre_agg" for p in probes2)


def test_classify_pre_agg_fanout_signature():
    w = classify_waterfall(
        [
            {"label": "scan", "rows": 500},
            {
                "label": "entrée GROUP BY",
                "boundary": "pre_agg",
                "rows": 340,
                "distinct_rows": 90,
            },
        ]
    )
    pa = w[1]
    # 340 lignes pour 90 clés distinctes = fan-out ×3,78 sur l'axe de l'agrégat.
    assert pa["agg_fanout_ratio"] == 3.78
    assert pa["verdict"] == "fan_out"
    # Pas de ratio de préfixe sur cette frontière (le WHERE fausserait la comparaison).
    assert "fanout_ratio" not in pa


def test_classify_pre_agg_unique_axis_preserves():
    w = classify_waterfall([{"boundary": "pre_agg", "rows": 90, "distinct_rows": 90}])
    assert w[0]["verdict"] == "preserves"
