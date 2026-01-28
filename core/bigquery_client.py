"""BigQuery client wrapper for database operations."""
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import pandas as pd
from google.cloud import bigquery
print(bigquery.__version__)

from google.cloud.bigquery import SchemaField

from config.settings import get_settings
from .sql_validator import SQLValidator, SQLValidationError
from .exceptions import QueryExecutionError


@dataclass
class TableInfo:
    """Information about a BigQuery table."""

    project: str
    dataset: str
    table: str
    full_name: str
    num_rows: int | None
    num_bytes: int | None
    created: str | None
    modified: str | None
    description: str | None


@dataclass
class ColumnInfo:
    """Information about a table column."""

    name: str
    data_type: str
    mode: str  # NULLABLE, REQUIRED, REPEATED
    description: str | None


class BigQueryClient:
    """Wrapper for BigQuery operations with safety validations."""

    def __init__(self, project_id: str | None = None):
        """Initialize the BigQuery client."""
        settings = get_settings()
        self.project_id = project_id or settings.project_id
        self.max_bytes_billed = settings.max_bytes_billed
        self._client: bigquery.Client | None = None

    @property
    def client(self) -> bigquery.Client:
        """Lazy-load the BigQuery client."""
        if self._client is None:
            self._client = bigquery.Client(project=self.project_id)
        return self._client

    def list_datasets(self) -> list[str]:
        """List all datasets in the project."""
        datasets = list(self.client.list_datasets())
        return [ds.dataset_id for ds in datasets]

    def list_tables(self, dataset_id: str) -> list[TableInfo]:
        """List all tables in a dataset with metadata."""
        dataset_ref = self.client.dataset(dataset_id)
        tables = list(self.client.list_tables(dataset_ref))

        table_infos = []
        for table in tables:
            full_table = self.client.get_table(table)
            table_infos.append(
                TableInfo(
                    project=self.project_id,
                    dataset=dataset_id,
                    table=table.table_id,
                    full_name=f"{self.project_id}.{dataset_id}.{table.table_id}",
                    num_rows=full_table.num_rows,
                    num_bytes=full_table.num_bytes,
                    created=str(full_table.created) if full_table.created else None,
                    modified=str(full_table.modified) if full_table.modified else None,
                    description=full_table.description,
                )
            )

        return table_infos

    def get_table_schema(self, dataset_id: str, table_id: str) -> list[ColumnInfo]:
        """Get the schema of a table."""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self.client.get_table(table_ref)

        return [
            ColumnInfo(
                name=field.name,
                data_type=field.field_type,
                mode=field.mode,
                description=field.description,
            )
            for field in table.schema
        ]

    def get_table_info(self, dataset_id: str, table_id: str) -> TableInfo:
        """Get detailed information about a specific table."""
        table_ref = f"{self.project_id}.{dataset_id}.{table_id}"
        table = self.client.get_table(table_ref)

        return TableInfo(
            project=self.project_id,
            dataset=dataset_id,
            table=table_id,
            full_name=table_ref,
            num_rows=table.num_rows,
            num_bytes=table.num_bytes,
            created=str(table.created) if table.created else None,
            modified=str(table.modified) if table.modified else None,
            description=table.description,
        )

    def execute_query(
        self,
        sql: str,
        validate: bool = True,
        dry_run: bool = False,
    ) -> pd.DataFrame:
        """
        Execute a SQL query and return results as a DataFrame.

        Args:
            sql: The SQL query to execute
            validate: Whether to validate the SQL is SELECT-only
            dry_run: If True, only validate without executing

        Returns:
            Query results as a pandas DataFrame

        Raises:
            SQLValidationError: If validation is enabled and SQL is not SELECT-only
            QueryExecutionError: If the query fails to execute
        """
        # Validate SQL if requested
        if validate:
            try:
                sql = SQLValidator.validate(sql)
            except SQLValidationError as e:
                raise e

        # Configure job
        job_config = bigquery.QueryJobConfig(
            maximum_bytes_billed=self.max_bytes_billed,
            dry_run=dry_run,
        )

        try:
            query_job = self.client.query(sql, job_config=job_config)

            if dry_run:
                # Return empty DataFrame for dry runs
                return pd.DataFrame()

            # Wait for results
            result = query_job.result()
            return result.to_dataframe()

        except Exception as e:
            error_message = str(e)
            # Extract useful error info from BigQuery errors
            if hasattr(e, "errors") and e.errors:
                error_message = e.errors[0].get("message", str(e))

            raise QueryExecutionError(
                message=f"Query execution failed: {error_message}",
                query=sql,
                original_error=e,
            )

    def validate_query_syntax(self, sql: str) -> tuple[bool, str | None]:
        """
        Validate SQL syntax using BigQuery dry-run.

        Args:
            sql: The SQL query to validate

        Returns:
            Tuple of (is_valid, error_message)
        """
        try:
            self.execute_query(sql, validate=True, dry_run=True)
            return True, None
        except SQLValidationError as e:
            return False, str(e)
        except QueryExecutionError as e:
            return False, str(e)

    def get_schema_for_context(self, dataset_id: str, table_ids: list[str] | None = None) -> dict[str, Any]:
        """
        Get schema information formatted for LLM context.

        Args:
            dataset_id: The dataset to get schema for
            table_ids: Optional list of specific tables (defaults to all)

        Returns:
            Dictionary with schema information
        """
        if table_ids is None:
            tables = self.list_tables(dataset_id)
            table_ids = [t.table for t in tables]

        schema_info = {
            "project": self.project_id,
            "dataset": dataset_id,
            "tables": {},
        }

        for table_id in table_ids:
            columns = self.get_table_schema(dataset_id, table_id)
            table_info = self.get_table_info(dataset_id, table_id)

            schema_info["tables"][table_id] = {
                "full_name": f"`{self.project_id}.{dataset_id}.{table_id}`",
                "row_count": table_info.num_rows,
                "description": table_info.description,
                "columns": [
                    {
                        "name": col.name,
                        "type": col.data_type,
                        "mode": col.mode,
                        "description": col.description,
                    }
                    for col in columns
                ],
            }

        return schema_info


@lru_cache
def get_bigquery_client() -> BigQueryClient:
    """Get a cached BigQuery client instance."""
    return BigQueryClient()
