import json
from typing import List, Dict

from google.cloud import bigquery
from google.cloud.exceptions import GoogleCloudError
from sqlglot import exp
from sqlmesh.core.config import (
    Config,
    BigQueryConnectionConfig,
    GatewayConfig,
    ModelDefaultsConfig,
)
from sqlmesh.core.context import Context
from sqlmesh.core.model import SqlModel
from sqlmesh.core.table_diff import RowDiff, SchemaDiff


class BigQueryTableManager:
    """
    BigQueryTableManager is a class that manages the creation of BigQuery tables,
    environments, and the comparison of tables across different environments using SQLMesh.
    This class implements the Singleton pattern to ensure a single instance is used.
    """

    _instance = None

    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super(BigQueryTableManager, cls).__new__(cls)
        return cls._instance

    def __init__(self, schema: dict, project: str, token: str, location: str = "US"):
        if not hasattr(self, "initialized"):  # Prevent re-initialization
            self.mapping_schema = schema

            bigquery_connection = BigQueryConnectionConfig(
                project=project, location=location, token=token
            )
            self.bigquery_connection = bigquery_connection
            self.custom_config = Config(
                gateways={
                    "default_gateway": GatewayConfig(connection=bigquery_connection)
                },
                default_connection=bigquery_connection,
                default_gateway="default_gateway",
                project=project,
                model_defaults=ModelDefaultsConfig(dialect="bigquery"),
            )

            self.context = Context(config=self.custom_config)
            self.initialized = True

    @classmethod
    def get_instance(cls, schema: dict, project: str, token: str, location: str = "US"):
        """
        Returns the single instance of BigQueryTableManager.

        Args:
            schema (dict): A dictionary representing the schema of the BigQuery tables.
            project (str): The Google Cloud project ID.
            token (str): The Google Cloud token.
            location (str): The location of the BigQuery data, defaults to "US".

        Returns:
            BigQueryTableManager: The singleton instance of the BigQueryTableManager class.
        """
        if cls._instance is None:
            cls._instance = BigQueryTableManager(schema, project, token, location)
        return cls._instance

    def create_table(
        self,
        table_name: str,
        query_expression: exp.Query,
        environment: str,
        grains: list = None,
    ):
        """
        Creates a SQL model representing a BigQuery table.

        Args:
            table_name (str): The name of the table to create.
            query_expression (exp.Select): The SQL query expression defining the table structure and data.
            grains (list): The grains (partition keys) for the table.
            environment (str): The environment to plan (e.g., "dev", "prod").
        """
        sql_model = SqlModel(
            name=table_name,
            query=query_expression,
            mapping_schema=self.mapping_schema,
            grains=grains,
        )

        self.context.upsert_model(sql_model)
        self.context.plan(environment=environment, auto_apply=True, no_prompts=True)

    def compare_environments(
        self, table_name: str, source_env: str = "prod", target_env: str = "dev"
    ) -> dict:
        """
        Compares the schema and data between two environments for a specified table.

        Args:
            source_env (str): The source environment for comparison (e.g., "prod").
            target_env (str): The target environment for comparison (e.g., "dev").
            table_name (str): The name of the table to compare.

        Returns:
            dict: A dictionary containing the schema differences and row differences.

        Raises:
            ValueError: If the comparison fails due to an invalid environment or table name.
        """
        try:
            diff = self.context.table_diff(
                source=source_env,
                target=target_env,
                model_or_snapshot=table_name,
                show=False,
            )

            schema_diff: SchemaDiff = diff.schema_diff()
            row_diff: RowDiff = diff.row_diff()

            return {
                "schema_diff": {
                    "added": schema_diff.added,
                    "removed": schema_diff.removed,
                    "modified": schema_diff.modified,
                },
                "joined_sample": json.dumps(
                    row_diff.joined_sample.to_dict(orient="records")
                ),
                "dev_only_sample": json.dumps(
                    row_diff.t_sample.to_dict(orient="records")
                ),
                "prod_only_sample": json.dumps(
                    row_diff.s_sample.to_dict(orient="records")
                ),
            }
        except ValueError as e:
            raise ValueError(f"Comparison failed: {e}")

    def list_datasets_and_tables(self, input_value: str, max_results: int = 10) -> list:
        """
        Lists up to a maximum number of datasets and their tables in the BigQuery project
        associated with the manager instance.

        Args:
            input_value (str): Optional filter for dataset_id.
            max_results (int): The maximum number of results to return.

        Returns:
            list: A list of strings in the format 'project.dataset' for datasets and
                  'project.dataset.table' for tables.
        """
        try:
            # Use the BigQuery connection to fetch datasets
            connection_config: BigQueryConnectionConfig = self.bigquery_connection
            client = connection_config._static_connection_kwargs.get("client")

            results = []

            # Fetch all datasets
            datasets = client.list_datasets()

            for dataset in datasets:
                dataset_name = f"{dataset.project}.{dataset.dataset_id}"

                # Add dataset to the results if it matches the filter
                if not input_value or input_value.lower() in dataset.dataset_id.lower():
                    results.append(dataset_name)

                    # Fetch tables for the dataset
                    dataset_ref = client.dataset(dataset.dataset_id)
                    tables = client.list_tables(dataset_ref)

                    for table in tables:
                        table_name = f"{dataset_name}.{table.table_id}"
                        results.append(table_name)

                        # Stop when max_results is reached
                        if len(results) >= max_results:
                            return results
            return results
        except Exception as e:
            raise RuntimeError(f"Failed to list datasets and tables: {e}")

    def get_schema_from_dataset(self, inputs: List[str]) -> List[Dict[str, str]]:
        """
        Retrieves the schema for a list of BigQuery datasets or tables using UNION ALL.

        Args:
            inputs (List[str]): List of datasets or tables in the format `project.database` or `project.database.table`.

        Returns:
            List[Dict[str, str]]: A list of dictionaries containing schema details for all provided inputs.
                Each dictionary includes:
                - `table_catalog`: Catalog name (project).
                - `table_schema`: Schema name (dataset).
                - `table_name`: Name of the table.
                - `field_path`: Path of the column (nested or flat).
                - `data_type`: Data type of the column.
                - `description`: Description of the column (if available).

        Raises:
            ValueError: If any input format is invalid or does not exist.
            RuntimeError: If there is an error executing the query.
        """
        try:
            # Get BigQuery client from connection config
            connection_config: BigQueryConnectionConfig = self.bigquery_connection
            client = connection_config._static_connection_kwargs.get("client")

            if not client or not isinstance(client, bigquery.Client):
                raise ValueError("Invalid BigQuery client configuration.")

            # Build the query with UNION ALL
            union_queries = []
            for input_item in inputs:
                parts = input_item.split(".")
                if len(parts) < 2:
                    raise ValueError(
                        f"Invalid input format: '{input_item}'. Expected `project.database` or `project.database.table`."
                    )

                project = parts[0]
                dataset = parts[1]
                table = parts[2] if len(parts) == 3 else None

                if table:
                    # Query for a specific table
                    query = f"""
                        SELECT 
                            table_catalog, table_schema, table_name, field_path, data_type, description
                        FROM 
                            `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                        WHERE 
                            table_name = '{table}'
                        """
                else:
                    # Query for all tables in the dataset
                    query = f"""
                        SELECT 
                            table_catalog, table_schema, table_name, field_path, data_type, description
                        FROM 
                            `{project}.{dataset}.INFORMATION_SCHEMA.COLUMN_FIELD_PATHS`
                        """
                union_queries.append(query)

            # Combine all queries using UNION ALL
            final_query = " UNION ALL ".join(union_queries)

            # Execute the combined query
            query_job = client.query(final_query)
            results = query_job.result()

            # Format the results
            schema_details = [
                {
                    "table_catalog": row.table_catalog,
                    "table_schema": row.table_schema,
                    "table_name": row.table_name,
                    "field_path": row.field_path,
                    "data_type": row.data_type,
                    "description": row.description,
                }
                for row in results
            ]

            return schema_details

        except GoogleCloudError as e:
            raise RuntimeError(f"Error accessing BigQuery: {e}")
