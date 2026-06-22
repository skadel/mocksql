"""Régression — une colonne *mesure* (argument d'agrégat) ne doit jamais partir
en Faker-fill.

Cas réel : bq018 (examples/spider). Le SQL agrège `SUM(cumulative_confirmed)`,
puis applique `LAG` + ranking. `cumulative_confirmed` n'est ni dans un `WHERE`,
ni dans le `GROUP BY` (qui porte sur `date`) → le simplifier ne la voit pas
comme contrainte → elle tombait dans `faker_cols` et était remplie de valeurs
arbitraires (5269→2098, 6603→6238…) déconnectées de la description du scénario
(« 100 puis 150 cas »). Résultat : désync description↔données détectée comme
`bad_input_description`, test invalide.

Comportement attendu : la colonne argument d'un agrégat (SUM/AVG/COUNT/MIN/MAX/
STDDEV…) est marquée contrainte → c'est le LLM qui choisit ses valeurs en
cohérence avec la description, jamais Faker.
"""

from build_query.constraint_simplifier import simplify
from build_query.examples_generator import _compute_faker_columns


# ─── 1. La mesure agrégée n'est PAS Faker-fill (régression bq018) ────────────


def test_aggregate_measure_is_not_faker_filled():
    sql = (
        "SELECT date, SUM(cumulative_confirmed) AS cases "
        "FROM `bigquery-public-data.covid19_open_data.covid19_open_data` "
        "WHERE country_name = 'United States of America' "
        "AND date BETWEEN '2020-03-01' AND '2020-04-30' "
        "GROUP BY date"
    )
    used_columns = [
        {
            "project": "bigquery-public-data",
            "database": "covid19_open_data",
            "table": "covid19_open_data",
            "used_columns": ["country_name", "cumulative_confirmed", "date"],
        }
    ]
    base_tables = {"covid19_open_data"}
    sim_result = simplify(sql, dialect="bigquery")

    faker = _compute_faker_columns(
        sim_result, used_columns, base_tables, sql=sql, dialect="bigquery"
    )

    uc_key = "covid19_open_data_covid19_open_data"
    assert "cumulative_confirmed" not in faker.get(uc_key, set()), (
        "cumulative_confirmed est l'argument de SUM(...) — la mesure que la "
        "description du test épingle ('100 puis 150 cas'). La Faker-fill avec "
        "des valeurs arbitraires crée une désync description↔données "
        "(bad_input_description)."
    )


# ─── 2. Garde — un dimension de remplissage reste bien Faker-fill ────────────


def test_non_aggregate_passthrough_still_faker_filled():
    """Le fix ne doit pas geler tout : une colonne ni filtrée, ni agrégée, ni
    groupée reste éligible au Faker-fill."""
    sql = "SELECT id, label FROM `ds.t` WHERE status = 'active'"
    used_columns = [
        {
            "database": "ds",
            "table": "t",
            "used_columns": ["id", "label", "status"],
        }
    ]
    base_tables = {"t"}
    sim_result = simplify(sql, dialect="bigquery")

    faker = _compute_faker_columns(
        sim_result, used_columns, base_tables, sql=sql, dialect="bigquery"
    )

    filled = faker.get("ds_t", set())
    assert "label" in filled, (
        "label est une dimension de remplissage (ni filtre, ni agrégat, ni "
        "GROUP BY) — elle doit rester Faker-fill"
    )
