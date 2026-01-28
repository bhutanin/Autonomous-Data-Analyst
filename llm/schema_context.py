"""Schema context extraction for LLM prompts."""
from typing import Any

from core.bigquery_client import BigQueryClient, ColumnInfo


class SchemaContextBuilder:
    """Builds formatted schema context for LLM prompts."""

    def __init__(self, client: BigQueryClient | None = None):
        """Initialize with a BigQuery client."""
        self.client = client or BigQueryClient()

    def build_context(
        self,
        dataset_id: str,
        table_ids: list[str] | None = None,
        include_sample_values: bool = False,
        include_row_counts: bool = True,
    ) -> str:
        """
        Build a formatted schema context string for LLM prompts.

        Args:
            dataset_id: The dataset to get schema for
            table_ids: Optional list of specific tables
            include_sample_values: Whether to include sample values
            include_row_counts: Whether to include row counts

        Returns:
            Formatted schema string
        """
        schema_info = self.client.get_schema_for_context(dataset_id, table_ids)
        return self._format_schema(schema_info, include_sample_values, include_row_counts)

    def _format_schema(
        self,
        schema_info: dict[str, Any],
        include_sample_values: bool,
        include_row_counts: bool,
    ) -> str:
        """Format schema info as a readable string."""
        lines = []
        lines.append(f"Project: {schema_info['project']}")
        lines.append(f"Dataset: {schema_info['dataset']}")
        lines.append("")

        for table_name, table_data in schema_info["tables"].items():
            lines.append(f"### Table: {table_data['full_name']}")

            if table_data.get("description"):
                lines.append(f"Description: {table_data['description']}")

            if include_row_counts and table_data.get("row_count"):
                lines.append(f"Row count: {table_data['row_count']:,}")

            lines.append("\nColumns:")
            for col in table_data["columns"]:
                col_line = f"  - `{col['name']}` ({col['type']}"
                if col["mode"] != "NULLABLE":
                    col_line += f", {col['mode']}"
                col_line += ")"
                if col.get("description"):
                    col_line += f" - {col['description']}"
                lines.append(col_line)

            lines.append("")

        return "\n".join(lines)

    def build_minimal_context(
        self,
        dataset_id: str,
        table_ids: list[str] | None = None,
    ) -> str:
        """
        Build a minimal schema context (just table and column names).

        Args:
            dataset_id: The dataset to get schema for
            table_ids: Optional list of specific tables

        Returns:
            Minimal formatted schema string
        """
        schema_info = self.client.get_schema_for_context(dataset_id, table_ids)

        lines = []
        for table_name, table_data in schema_info["tables"].items():
            columns = [f"`{c['name']}`" for c in table_data["columns"]]
            lines.append(f"{table_data['full_name']}: {', '.join(columns)}")

        return "\n".join(lines)

    def get_table_context(
        self,
        dataset_id: str,
        table_id: str,
    ) -> str:
        """
        Get detailed context for a single table.

        Args:
            dataset_id: The dataset containing the table
            table_id: The table to get context for

        Returns:
            Formatted table context
        """
        table_info = self.client.get_table_info(dataset_id, table_id)
        columns = self.client.get_table_schema(dataset_id, table_id)

        lines = []
        lines.append(f"Table: {table_info.full_name}")

        if table_info.description:
            lines.append(f"Description: {table_info.description}")

        if table_info.num_rows:
            lines.append(f"Row count: {table_info.num_rows:,}")

        if table_info.num_bytes:
            size_mb = table_info.num_bytes / (1024 * 1024)
            lines.append(f"Size: {size_mb:.2f} MB")

        lines.append("\nColumns:")
        for col in columns:
            lines.append(f"  - `{col.name}` ({col.data_type}, {col.mode})")
            if col.description:
                lines.append(f"    Description: {col.description}")

        return "\n".join(lines)

    def extract_relevant_tables(
        self,
        question: str,
        dataset_id: str,
        all_tables: list[str],
    ) -> list[str]:
        """
        Try to identify tables relevant to a user's question.

        This is a simple heuristic approach - looks for table names
        mentioned in the question.

        Args:
            question: The user's question
            dataset_id: The dataset to search
            all_tables: List of all table names

        Returns:
            List of potentially relevant table names
        """
        question_lower = question.lower()
        relevant = []

        for table in all_tables:
            # Check if table name appears in question
            table_lower = table.lower()

            # Direct mention
            if table_lower in question_lower:
                relevant.append(table)
                continue

            # Singular/plural handling
            if table_lower.endswith("s") and table_lower[:-1] in question_lower:
                relevant.append(table)
                continue

            if table_lower + "s" in question_lower:
                relevant.append(table)
                continue

        # If no matches, return all tables (let the LLM figure it out)
        return relevant if relevant else all_tables
