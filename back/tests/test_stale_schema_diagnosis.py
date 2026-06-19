"""Garde-fou : une qualify-error 'Unknown column: X' due à une table étoilée
(SELECT * FROM <table>) dont le schéma en cache ne contient pas X doit être
diagnostiquée comme cache périmé (→ fail-fast refresh-schemas), pas masquée par
un fallback silencieux sur le SQL brut (qui génère un test cassé).

Cas réel : c4.sql, CTE `refcomm = SELECT *, ROW_NUMBER() OVER (… ref_comm.NO_CONTRAT_COMMERCANT)
FROM DS_MR_DASHBOARD_RESEAU ref_comm`, schéma DS_MR tronqué à 1 colonne.
"""

from cli.generate import diagnose_stale_schema_from_qualify_error

DIALECT = "bigquery"

# DS_MR étoilée dans un CTE, schéma tronqué à id_immatriculation.
C4_LIKE_SQL = """
WITH refcomm AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY ref_comm.no_contrat_commercant) AS nb
  FROM ds.DS_MR_DASHBOARD_RESEAU ref_comm
)
SELECT no_contrat_commercant FROM refcomm
"""

SCHEMAS_THIN = [
    {
        "table_name": "proj.ds.DS_MR_DASHBOARD_RESEAU",
        "columns": [{"name": "id_immatriculation"}],
    },
]


def test_fires_on_starred_table_missing_column():
    culprits = diagnose_stale_schema_from_qualify_error(
        C4_LIKE_SQL, SCHEMAS_THIN, DIALECT, "Unknown column: no_contrat_commercant"
    )
    assert culprits == ["proj.ds.DS_MR_DASHBOARD_RESEAU"]


def test_silent_when_column_present_in_starred_schema():
    schemas_full = [
        {
            "table_name": "proj.ds.DS_MR_DASHBOARD_RESEAU",
            "columns": [
                {"name": "id_immatriculation"},
                {"name": "no_contrat_commercant"},
            ],
        }
    ]
    culprits = diagnose_stale_schema_from_qualify_error(
        C4_LIKE_SQL, schemas_full, DIALECT, "Unknown column: no_contrat_commercant"
    )
    assert culprits == []


def test_silent_when_error_is_not_unknown_column():
    culprits = diagnose_stale_schema_from_qualify_error(
        C4_LIKE_SQL, SCHEMAS_THIN, DIALECT, "Ambiguous reference to alias t"
    )
    assert culprits == []


def test_pins_culprit_by_qualifier_not_other_starred_tables():
    """Plusieurs tables étoilées, mais une seule (via son alias) qualifie la colonne
    irrésoluble → seule celle-là est signalée (pas de bruit sur les autres)."""
    sql = """
    WITH refcomm AS (
      SELECT * FROM ds.DS_MR_DASHBOARD_RESEAU ref_comm
    ),
    mcc AS (
      SELECT * FROM ds.MCC_SANTE mcc_sante
    )
    SELECT ref_comm.no_contrat_commercant FROM refcomm
    JOIN mcc ON TRUE
    """
    schemas = [
        {
            "table_name": "proj.ds.DS_MR_DASHBOARD_RESEAU",
            "columns": [{"name": "id_immatriculation"}],
        },
        {"table_name": "proj.ds.MCC_SANTE", "columns": [{"name": "mcc"}]},
    ]
    culprits = diagnose_stale_schema_from_qualify_error(
        sql, schemas, DIALECT, "Unknown column: no_contrat_commercant"
    )
    assert culprits == ["proj.ds.DS_MR_DASHBOARD_RESEAU"]


def test_silent_when_table_not_starred():
    sql = "SELECT a.no_contrat_commercant FROM ds.DS_MR_DASHBOARD_RESEAU a"
    culprits = diagnose_stale_schema_from_qualify_error(
        sql, SCHEMAS_THIN, DIALECT, "Unknown column: no_contrat_commercant"
    )
    assert culprits == []
