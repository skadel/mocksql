"""
Script de validation manuelle pour c1.sql.

Simule ce que le LLM devrait maintenant générer avec les nouvelles hints
anti-join (banques_france.groupe doit être HORS de la liste BPCE pour que
le SIRET ne tombe PAS dans SIRET_ONUS).

Usage:
    cd back && python scripts/run_c1_manual.py
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import dotenv

dotenv.load_dotenv()

# ─── SQL cible ───────────────────────────────────────────────────────────────

C1_SQL_PATH = (
    Path(__file__).resolve().parents[2]
    / "examples"
    / "spider_complexified"
    / "models"
    / "c1.sql"
)
SQL = C1_SQL_PATH.read_text(encoding="utf-8")

SESSION = "c1val"

# ─── Schéma des tables (types corrects) ─────────────────────────────────────
# Seules les colonnes utilisées par c1.sql sont déclarées avec les bons types.

SCHEMA = [
    {
        "table_name": "MONETIQUE_Dataset_MR.DS_MR_DASHBOARD_RESEAU",
        "description": "",
        "columns": [
            {"name": "id_immatriculation", "type": "STRING", "mode": "NULLABLE"}
        ],
    },
    {
        "table_name": "MARKETING_Referentiels.banques_france",
        "description": "",
        "columns": [
            {"name": "code_banque", "type": "STRING", "mode": "NULLABLE"},
            {"name": "libelle", "type": "STRING", "mode": "NULLABLE"},
            {"name": "groupe", "type": "STRING", "mode": "NULLABLE"},
            {"name": "type", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "chef_file", "type": "STRING", "mode": "NULLABLE"},
            {"name": "event_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "remarque", "type": "STRING", "mode": "NULLABLE"},
            {"name": "partition_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "traite", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
    {
        "table_name": "MARKETING_Referentiels.banques",
        "description": "",
        "columns": [
            {"name": "code_banque", "type": "STRING", "mode": "NULLABLE"},
            {"name": "reseau", "type": "STRING", "mode": "NULLABLE"},
            {"name": "partition_date", "type": "DATE", "mode": "NULLABLE"},
        ],
    },
    {
        "table_name": "MARKETING_GR_source_ref_bpce.coface",
        "description": "",
        "columns": [
            {"name": "cosirt", "type": "STRING", "mode": "NULLABLE"},
            {"name": "coapna", "type": "STRING", "mode": "NULLABLE"},
            {"name": "copost", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ctcatj", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ddentr", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cotefa", "type": "STRING", "mode": "NULLABLE"},
            {"name": "livoin", "type": "STRING", "mode": "NULLABLE"},
            {"name": "licoan", "type": "STRING", "mode": "NULLABLE"},
            {"name": "liras2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "liensc", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cotela", "type": "STRING", "mode": "NULLABLE"},
            {"name": "licomm", "type": "STRING", "mode": "NULLABLE"},
            {"name": "partition_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "coapet", "type": "STRING", "mode": "NULLABLE"},
            {"name": "mtcaht", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "ctsieg", "type": "STRING", "mode": "NULLABLE"},
            {"name": "qteff1", "type": "INT64", "mode": "NULLABLE"},
            {"name": "cosire", "type": "STRING", "mode": "NULLABLE"},
            {"name": "coiris", "type": "STRING", "mode": "NULLABLE"},
            {"name": "ctsene", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "ctfmtr", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dafms", "type": "DATE", "mode": "NULLABLE"},
            {"name": "liraso", "type": "STRING", "mode": "NULLABLE"},
            {"name": "coetb", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cttca", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dacaht", "type": "DATE", "mode": "NULLABLE"},
            {"name": "lilien", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
    {
        "table_name": "MARKETING_GR_source_ref_bpce.naf2",
        "description": "",
        "columns": [
            {"name": "niv5", "type": "STRING", "mode": "NULLABLE"},
            {"name": "niv1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "niv4", "type": "STRING", "mode": "NULLABLE"},
            {"name": "libelle_niv2_division", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "niv2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "niv3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "libelle_niv5_sous_classe", "type": "STRING", "mode": "NULLABLE"},
            {"name": "libelle_niv4_classe", "type": "STRING", "mode": "NULLABLE"},
            {"name": "partition_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "libelle_niv3_groupe", "type": "STRING", "mode": "NULLABLE"},
            {"name": "libelle_niv1_section", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
    {
        "table_name": "MARKETING_GR_source_ref_bpce.categories_juridiques",
        "description": "",
        "columns": [
            {"name": "catg_jurd_niv3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "libl_natr_jurd_niv1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "libl_natr_jurd_niv3", "type": "STRING", "mode": "NULLABLE"},
            {"name": "partition_date", "type": "DATE", "mode": "NULLABLE"},
            {"name": "catg_jurd_niv1", "type": "STRING", "mode": "NULLABLE"},
            {"name": "marche_catjur", "type": "STRING", "mode": "NULLABLE"},
            {"name": "libl_natr_jurd_niv2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "catg_jurd_niv2", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
    {
        "table_name": "MARKETING_GR_source_ref_bpce.code_mcc",
        "description": "",
        "columns": [
            {"name": "code_professionnel_mcc", "type": "STRING", "mode": "NULLABLE"},
            {"name": "domaine", "type": "STRING", "mode": "NULLABLE"},
            {"name": "partition_date", "type": "DATE", "mode": "NULLABLE"},
        ],
    },
    {
        "table_name": "MONETIQUE_Dataset_Porteur.DS_RCOMP_DASHBOARD_RESEAU",
        "description": "",
        "columns": [
            {
                "name": "cd_banque_acquereur_calcule",
                "type": "STRING",
                "mode": "NULLABLE",
            },
            {"name": "cd_banque_emetteur", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cd_cycle_operation", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cd_ert", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cd_mcc", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cd_nature_operation", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cd_pays_commercant", "type": "STRING", "mode": "NULLABLE"},
            {"name": "dt_extraction", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "dt_transaction", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "mt_brut_transaction", "type": "FLOAT64", "mode": "NULLABLE"},
            {"name": "no_siret", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
    {
        "table_name": "MARKETING_Referentiels.territoire_prospect",
        "description": "",
        "columns": [
            {"name": "dpt", "type": "STRING", "mode": "NULLABLE"},
            {"name": "processing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
            {"name": "lib_territoire_bp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "lib_territoire_ce", "type": "STRING", "mode": "NULLABLE"},
            {"name": "part_banque_2", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cb_territoire_bp", "type": "STRING", "mode": "NULLABLE"},
            {"name": "cb_territoire_ce", "type": "STRING", "mode": "NULLABLE"},
        ],
    },
]

# ─── Données de test ─────────────────────────────────────────────────────────
# Règle clé:
#  - CD_BANQUE_ACQUEREUR_CALCULE = "999" → banques_france.groupe = "Crédit Mutuel"
#    → CASE … ELSE groupe → 'Crédit Mutuel' (hors liste BPCE)
#    → proportion_ca = 100% mais groupe hors liste → PAS dans SIRET_ONUS → anti-join ✓
#  - CD_BANQUE_EMETTEUR = "001" → banques.reseau = 'BP' → RESEAU match → WHERE ✓
#  - partition_date = "2025-12-31" → MAX = "2025-12-31" ≤ "2026-01-01" → pinning ✓

TEST_DATA = {
    "DS_MR_DASHBOARD_RESEAU": [],
    "banques_france": [
        {
            "code_banque": "999",
            "libelle": "Crédit Mutuel Île-de-France",
            "groupe": "Crédit Mutuel",
            "type": "banque",
            "processing_time": "2026-01-01T00:00:00",
            "chef_file": "CM",
            "event_date": "2025-01-01",
            "remarque": None,
            "partition_date": "2025-12-31",
            "traite": "oui",
        },
        {
            "code_banque": "001",
            "libelle": "Banque Populaire Méditerranée",
            "groupe": "Banque Populaire",
            "type": "banque",
            "processing_time": "2026-01-01T00:00:00",
            "chef_file": "BP",
            "event_date": "2025-01-01",
            "remarque": None,
            "partition_date": "2025-12-31",
            "traite": "oui",
        },
    ],
    "banques": [
        {
            "code_banque": "001",
            "reseau": "BP",
            "partition_date": "2025-12-31",
        }
    ],
    "coface": [
        {
            "cosirt": "12345678901234",
            "coapna": "4711Z",
            "copost": "75001",
            "ctcatj": "5499",
            "ddentr": "15Jan2020",
            "cotefa": "0123456789",
            "livoin": "3 rue de la Paix",
            "licoan": None,
            "liras2": None,
            "liensc": "Boulangerie du Marché",
            "cotela": "0987654321",
            "licomm": None,
            "partition_date": "2025-12-31",
            "coapet": "47.1Z",
            "mtcaht": 250000.0,
            "ctsieg": "S",
            "qteff1": 5,
            "cosire": "123456789",
            "coiris": "751010101",
            "ctsene": "commerce",
            "processing_time": "2026-01-01T00:00:00",
            "ctfmtr": None,
            "dafms": "2025-01-01",
            "liraso": "BOULANGERIE DU MARCHE SARL",
            "coetb": "001234",
            "cttca": "N",
            "dacaht": "2024-12-31",
            "lilien": "Paris 1er",
        }
    ],
    "naf2": [
        {
            "niv5": "4711Z",
            "niv1": "G",
            "niv4": "471",
            "libelle_niv2_division": "Commerce de détail",
            "processing_time": "2026-01-01T00:00:00",
            "niv2": "47",
            "niv3": "471",
            "libelle_niv5_sous_classe": "Hypermarchés",
            "libelle_niv4_classe": "Commerce de détail à prédominance alimentaire",
            "partition_date": "2025-12-31",
            "libelle_niv3_groupe": "Commerce de détail alimentaire",
            "libelle_niv1_section": "Commerce",
        }
    ],
    "categories_juridiques": [
        {
            "catg_jurd_niv3": "5499",
            "processing_time": "2026-01-01T00:00:00",
            "libl_natr_jurd_niv1": "Personne morale de droit privé",
            "libl_natr_jurd_niv3": "SARL",
            "partition_date": "2025-12-31",
            "catg_jurd_niv1": "5",
            "marche_catjur": "PME",
            "libl_natr_jurd_niv2": "Société commerciale",
            "catg_jurd_niv2": "54",
        }
    ],
    "code_mcc": [
        {
            "code_professionnel_mcc": "5411",
            "domaine": "Alimentaire",
            "partition_date": "2025-12-31",
        }
    ],
    "DS_RCOMP_DASHBOARD_RESEAU": [
        {
            "cd_banque_acquereur_calcule": "999",
            "cd_banque_emetteur": "001",
            "cd_cycle_operation": "I",
            "cd_ert": "10",
            "cd_mcc": "5411",
            "cd_nature_operation": "D",
            "cd_pays_commercant": "250",
            "dt_extraction": "2025-06-15T00:00:00",
            "dt_transaction": "2025-06-14T00:00:00",
            "mt_brut_transaction": 150.0,
            "no_siret": "12345678901234",
        }
    ],
    "territoire_prospect": [
        {
            "dpt": "75",
            "processing_time": "2026-01-01T00:00:00",
            "lib_territoire_bp": "Territoire BP Paris",
            "lib_territoire_ce": "Territoire CE Paris",
            "part_banque_2": "2",
            "cb_territoire_bp": "10107",
            "cb_territoire_ce": "10207",
        }
    ],
}


async def run_test():
    from utils.examples import (
        create_test_tables,
        run_query_on_test_dataset,
        initialize_duckdb,
    )
    from utils.insert_examples import insert_examples

    print(f"\n{'=' * 70}")
    print("  MockSQL c1.sql — validation manuelle des données anti-join")
    print(f"{'=' * 70}\n")
    print("Règle testée : banques_france.groupe = 'Crédit Mutuel' (hors BPCE)")
    print("  → proportion_ca = 100% mais groupe hors liste BPCE")
    print("  → PAS dans SIRET_ONUS → anti-join passe → résultat attendu ≥ 1 ligne\n")

    con = initialize_duckdb(":memory:")

    # Créer les tables avec les bons types
    duckdb_tables = create_test_tables(
        tables=SCHEMA,
        suffix=SESSION,
        con=con,
        dialect="bigquery",
        used_columns=None,  # toutes les colonnes
        overwrite=True,
    )

    if not duckdb_tables:
        print("[ERROR] Aucune table créée")
        return

    print(f"Tables créées : {[t['table_name'] for t in duckdb_tables]}\n")

    # Insérer les données
    # Clés = "{database}_{table}" SANS suffixe (format _uc_key du pipeline réel)
    data_for_insert = {
        "MONETIQUE_Dataset_MR_DS_MR_DASHBOARD_RESEAU": TEST_DATA[
            "DS_MR_DASHBOARD_RESEAU"
        ],
        "MARKETING_Referentiels_banques_france": TEST_DATA["banques_france"],
        "MARKETING_Referentiels_banques": TEST_DATA["banques"],
        "MARKETING_GR_source_ref_bpce_coface": TEST_DATA["coface"],
        "MARKETING_GR_source_ref_bpce_naf2": TEST_DATA["naf2"],
        "MARKETING_GR_source_ref_bpce_categories_juridiques": TEST_DATA[
            "categories_juridiques"
        ],
        "MARKETING_GR_source_ref_bpce_code_mcc": TEST_DATA["code_mcc"],
        "MONETIQUE_Dataset_Porteur_DS_RCOMP_DASHBOARD_RESEAU": TEST_DATA[
            "DS_RCOMP_DASHBOARD_RESEAU"
        ],
        "MARKETING_Referentiels_territoire_prospect": TEST_DATA["territoire_prospect"],
    }

    print("Insertion des données...")
    try:
        from utils.examples import execute_queries

        insert_queries = insert_examples(
            data_dict=data_for_insert,
            schemas=duckdb_tables,
            suffix=SESSION,
            used_columns=None,
        )
        execute_queries(list(insert_queries), con)
        print("  ✓ Données insérées\n")
    except Exception as e:
        print(f"  [ERROR] insert_examples: {e}")
        import traceback

        traceback.print_exc()
        return

    # Exécuter le SQL
    print("[EXEC] Exécution sur DuckDB...")
    try:
        result_df, duckdb_sql = await run_query_on_test_dataset(
            SQL, SESSION, "", "bigquery", con
        )
        n = len(result_df)
        print(f"\n[RESULT] {n} ligne(s) retournée(s)")
        if n > 0:
            print(f"Colonnes: {list(result_df.columns[:6])}...")
            print("Première ligne:")
            row = result_df.iloc[0]
            for col in list(result_df.columns[:10]):
                print(f"  {col}: {row[col]}")
            print("\n✅ SUCCÈS — l'anti-join est maintenant correctement géré !")
            print(
                "   Le LLM, guidé par la hint, devrait générer des données similaires."
            )
        else:
            print("\n❌ ÉCHEC — 0 lignes retournées")
            print("Diagostic DuckDB SQL (début):")
            print(duckdb_sql[:1000])
    except Exception as e:
        print(f"\n[ERROR] {e}")
        import traceback

        traceback.print_exc()


if __name__ == "__main__":
    asyncio.run(run_test())
