import asyncio
import re
from datetime import timedelta, datetime
from typing import List, Dict, Any, Optional

from dateutil import parser
from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from pandas import DataFrame

from models.env_variables import BQ_TEST_PROJECT, BQ_TEST_DATASET, BQ_SANDBOX_MODE
from utils.examples import modify_test_dataset_for_bigquery_exec


class BigQueryTestHelper:
    def __init__(
        self, project_id: str = BQ_TEST_PROJECT, dataset_id: str = BQ_TEST_DATASET
    ):
        self.client = bigquery.Client(project=project_id)
        self.dataset_id = dataset_id
        self.project_id = project_id

    def create_table(
        self,
        session_id: Optional[str],
        table_name_key: str,
        schema: List[bigquery.SchemaField],
        expiration_hours: Optional[int] = None,
    ):
        table_name_key = table_name_key
        if session_id:
            table_name = f"{table_name_key}_{session_id.replace('-', '_')}"
        else:
            table_name = table_name_key
        dataset_ref = self.client.dataset(self.dataset_id, project=self.project_id)
        table_ref = dataset_ref.table(table_name)

        # Check if table exists and delete if it does
        try:
            self.client.get_table(table_ref)
            self.client.delete_table(table_ref)
            print(f"Deleted existing table {table_name}")
        except NotFound:
            print(f"Table {table_name} does not exist, no need to deleeeeeete")

        # Create the new table
        table = bigquery.Table(table_ref, schema=schema)
        if expiration_hours is not None:
            expiration_time = datetime.now() + timedelta(hours=expiration_hours)
            table.expires = expiration_time
            print(
                f"Creating table {table.table_id} with expiration time {table.expires}"
            )
        else:
            print(f"Creating table {table.table_id} without expiration time")

        print("<<<<<<<<<<<<<<<<<<<table")
        print(table)
        print("<<<<<<<<<<<<<<<<<<<schema")
        print(schema)
        self.client.create_table(table)
        print(f"Created table {table.table_id}")

    def build_schema(self, columns: List[dict]) -> List[bigquery.SchemaField]:
        """
        Construit le schéma BigQuery de manière récursive en utilisant le dot-notation.
        Assumes que 'columns' contient tous les champs (ex: 'totals', 'totals.hits', etc.)
        """
        root_fields = [col for col in columns if "." not in col["name"]]

        def get_subfields(parent_name):
            parent_depth = parent_name.count(".")
            immediate_children = [
                col for col in columns
                if col["name"].startswith(f"{parent_name}.")
                and col["name"].count(".") == parent_depth + 1
            ]
            subfields = []
            for col in immediate_children:
                field_type = self.convert_type(col["type"])
                mode = col.get("mode", "NULLABLE")
                nested_fields = get_subfields(col["name"]) if field_type == "RECORD" else []
                subfields.append(
                    bigquery.SchemaField(
                        name=col["name"].split(".")[-1],
                        field_type=field_type,
                        mode=mode,
                        fields=nested_fields,
                    )
                )
            return subfields

        final_schema = []
        for col in root_fields:
            field_type = self.convert_type(col["type"])
            mode = col.get("mode", "NULLABLE")
            nested_fields = get_subfields(col["name"]) if field_type == "RECORD" else []
            final_schema.append(
                bigquery.SchemaField(
                    name=col["name"],
                    field_type=field_type,
                    mode=mode,
                    fields=nested_fields,
                )
            )
        return final_schema

    def convert_type(self, type_str: str) -> str:
        type_str = type_str.upper()
        if "STRUCT" in type_str or "RECORD" in type_str:
            return "RECORD"
        mapping = {
            "INT64": "INTEGER",
            "STRING": "STRING",
            "BOOL": "BOOLEAN",
            "FLOAT64": "FLOAT",
            "TIMESTAMP": "TIMESTAMP",
            "DATE": "DATE",
            "NUMERIC": "NUMERIC",
            "GEOGRAPHY": "GEOGRAPHY",
        }
        clean = type_str.replace("ARRAY<", "").replace(">", "").strip()
        return mapping.get(clean, "STRING")
    
    async def insert_data(
        self,
        session_id: str,
        table_name_key: str,
        records: List[Dict[str, Any]],
        schema: List[bigquery.SchemaField],
    ):
        if not records or len(records) == 0:
            print("Records are either None or empty")
            return

        table_name = f"{table_name_key}_{session_id.replace('-', '_')}"
        dataset_ref = self.client.dataset(self.dataset_id, project=self.project_id)
        table_ref = dataset_ref.table(table_name)

        # Extract timestamp fields and date fields from schema
        timestamp_fields = [
            field.name for field in schema if field.field_type == "TIMESTAMP"
        ]
        date_fields = [field.name for field in schema if field.field_type == "DATE"]

        # Ensure all dates and timestamps are in the correct format
        for record in records:
            for field in timestamp_fields:
                if field in record and isinstance(record[field], str):
                    try:
                        dt = parser.parse(record[field])
                        record[field] = dt.strftime("%Y-%m-%d %H:%M:%S")
                    except ValueError:
                        print(
                            f"Error parsing timestamp field {field} with value {record[field]}"
                        )
            for field in date_fields:
                if field in record and isinstance(record[field], str):
                    try:
                        dt = parser.parse(record[field])
                        record[field] = dt.strftime("%Y-%m-%d")
                    except ValueError:
                        print(
                            f"Error parsing date field {field} with value {record[field]}"
                        )

        max_attempts = 10
        for attempt in range(max_attempts):
            try:
                # Insert rows directly as list of dictionaries
                errors = self.client.insert_rows_json(table_ref, records)
                if errors:
                    print(f"Errors occurred while inserting rows: {errors}")
                else:
                    print(f"Inserted {len(records)} rows into {table_name}")
                return  # Exit after successful insert
            except NotFound:
                print(
                    f"Table {table_name} not found, retrying insert ({attempt + 1}/{max_attempts})..."
                )
                await asyncio.sleep(5)  # Increase the sleep time
        raise RuntimeError(
            f"Failed to insert data into {table_name} after {max_attempts} attempts."
        )

    async def create_and_insert_and_query(
        self,
        session_id: str,
        data_dict: Dict[str, List[Dict[str, Any]]],
        query: str,
        tables_and_columns: List[dict],
        overwrite=True,
    ) -> DataFrame:
        if overwrite:
            tasks = []

            for table in tables_and_columns:
                parts = table["table_name"].split(".")
                table_name_key = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
                if table_name_key in data_dict:
                    schema = self.build_schema(table["columns"])
                    self.create_table(
                        session_id, table_name_key, schema, expiration_hours=1
                    )
                    records = data_dict[table_name_key]
                    tasks.append(
                        self.insert_data(session_id, table_name_key, records, schema)
                    )

            await asyncio.gather(*tasks)
            # Execute the provided query
            await asyncio.sleep(3)
            # Await all insert_data tasks concurrently
        try:
            return self.execute_query(
                await self.query_on_test_dataset(query, session_id)
            )
        except Exception as e:
            # str(e) = message d'erreur qui contient potentiellement des noms de tables "test_project.test_dataset.table_sessionid"
            original_error_msg = revert_test_dataset_references_in_error(
                error_message=str(e),
                tables=[x["table_name"] for x in tables_and_columns],
                session_id=session_id,
                test_project=self.project_id,
                test_dataset=self.dataset_id,
            )
            print("Erreur (références restaurées) :", original_error_msg)
            raise e

    async def query_on_test_dataset(self, query, table_suffix):
        return modify_test_dataset_for_bigquery_exec(
            sql_query=query,
            session_id=table_suffix,
            dialect="bigquery",
            test_dataset=BQ_TEST_DATASET,
        )

    def _ensure_dataset_sandbox_config(self) -> None:
        """Set dataset-level expiration defaults required by BigQuery sandbox mode (< 60 days)."""
        dataset_ref = self.client.dataset(self.dataset_id, project=self.project_id)
        try:
            dataset = self.client.get_dataset(dataset_ref)
        except NotFound:
            raise ValueError(
                f"Le dataset BigQuery '{self.dataset_id}' est introuvable dans le projet '{self.project_id}'.\n"
                f"Étapes pour le créer :\n"
                f"  1. Dans la console GCP → BigQuery → sélectionnez le projet '{self.project_id}'\n"
                f"  2. Cliquez sur '+ Créer un dataset', nommez-le '{self.dataset_id}'\n"
                f"  3. Ou via CLI : bq mk --dataset {self.project_id}:{self.dataset_id}\n"
                f"Vérifiez aussi que BQ_TEST_PROJECT dans votre .env correspond bien à ce projet."
            )

        expiration_ms = 59 * 24 * 60 * 60 * 1000  # 59 days in ms
        dataset.default_table_expiration_ms = expiration_ms
        dataset.default_partition_expiration_ms = expiration_ms
        self.client.update_dataset(
            dataset, ["default_table_expiration_ms", "default_partition_expiration_ms"]
        )
        print(f"Dataset {self.dataset_id} sandbox expiration defaults set to 59 days")

    def create_empty_tables(self, tables_and_columns: List[dict]) -> None:
        expiration_hours = (
            59 * 24 if BQ_SANDBOX_MODE else None
        )  # 59 days, under sandbox 60-day limit
        if BQ_SANDBOX_MODE:
            self._ensure_dataset_sandbox_config()
        for table in tables_and_columns:
            parts = table["table_name"].split(".")
            table_name_key = "_".join(parts[-2:]) if len(parts) >= 2 else parts[-1]
            schema = self.build_schema(table["columns"])
            self.create_table(
                None, table_name_key, schema, expiration_hours=expiration_hours
            )

    def execute_query(self, query: str) -> DataFrame:
        query_job = self.client.query(query)
        results = query_job.result().to_dataframe()  # Wait for the job to complete.
        return results


def revert_test_dataset_references_in_error(
    error_message: str,
    tables: list,
    session_id: str,
    test_project: str,
    test_dataset: str,
) -> str:
    """
    Remplace, dans un message d'erreur, les références aux tables de test
    (ex: test_project.test_dataset.ma_table_sessionid) par le nom original
    (ex: project.dataset.ma_table).

    :param error_message: Le message d'erreur où faire le remplacement.
    :param tables: La liste des noms de tables source (ex: ["project.dataset.table", ...]).
    :param session_id: L'identifiant de session qui a été suffixé dans la table de test.
    :param test_project: Le project ID utilisé pour la table de test.
    :param test_dataset: Le dataset ID utilisé pour la table de test.
    :return: Le message d'erreur modifié.
    """

    # On construit un mapping inverse : { "test_project.test_dataset.ma_table_sessionid" : "project.dataset.ma_table" }
    # Éventuellement, gérer aussi les références partielles ("dataset.table_sessionid" -> "dataset.table") si besoin.
    reverse_mapping = {}

    # Construire la table de correspondance inverse
    for original_table in tables:
        # original_table est un nom complet "project.dataset.table" ou éventuellement "dataset.table"
        parts = original_table.split(".")
        table_name = parts[-1]  # Nom de la table (ex: "table")
        dataset_name = parts[-2] if len(parts) >= 2 else None
        project_name = parts[-3] if len(parts) == 3 else None

        # Nom de la table de test : dataset_table_suffix (cohérent avec create_empty_tables)
        test_table_suffix = session_id.replace("-", "_")
        table_name_key = f"{dataset_name}_{table_name}" if dataset_name else table_name
        test_full_name = (
            f"{test_project}.{test_dataset}.{table_name_key}_{test_table_suffix}"
        )

        # On enregistre le mapping vers le nom d’origine complet
        if project_name:
            original_full_name = f"{project_name}.{dataset_name}.{table_name}"
        else:
            original_full_name = f"{dataset_name}.{table_name}"  # cas partiel

        reverse_mapping[test_full_name] = original_full_name

        # Facultatif : si l’erreur peut aussi référencer "test_dataset.table_sessionid" sans le project
        test_partial_name = f"{test_dataset}.{table_name_key}_{test_table_suffix}"
        reverse_mapping[test_partial_name] = original_full_name

    # Prépare un pattern qui matche soit un nom complet, soit un nom partiel,
    # dans ses formes potentiellement backquotées.
    # Le but est de capturer quelque chose comme : test_project.test_dataset.table_sessionid
    # ou juste test_dataset.table_sessionid
    pattern = re.compile(
        r"(?P<fqtn>`?([\w-]+)`?\.`?([\w-]+)`?\.`?([\w-]+)`?)|(?P<pqtn>`?([\w-]+)`?\.`?([\w-]+)`?)"
    )

    def replacer(match):
        # match.group('fqtn') correspond au groupe pour le fully qualified table name
        # match.group('pqtn') correspond au groupe pour le partial qualified table name
        matched_str = match.group(0)

        # On retire les backquotes éventuels pour faire la comparaison de mapping
        matched_str_unquoted = matched_str.replace("`", "")

        # Si c'est un FQTN
        if match.group("fqtn"):
            if matched_str_unquoted in reverse_mapping:
                return reverse_mapping[matched_str_unquoted]
            return matched_str  # sinon on ne remplace pas
        # Sinon si c'est un PQTN
        elif match.group("pqtn"):
            if matched_str_unquoted in reverse_mapping:
                return reverse_mapping[matched_str_unquoted]
            return matched_str

        return matched_str  # Par sécurité, on retourne la chaine non modifiée

    # On applique le replacer sur le message d'erreur
    new_error_message = pattern.sub(replacer, error_message)
    return new_error_message
