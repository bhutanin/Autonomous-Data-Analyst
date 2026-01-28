"""Table profiling orchestration."""
from dataclasses import dataclass, field
from typing import Any

import pandas as pd

from core.bigquery_client import BigQueryClient, ColumnInfo, TableInfo
from core.exceptions import ProfilerError
from config.settings import get_settings
from .column_stats import (
    ColumnStatistics,
    compute_column_stats,
    generate_approx_top_values_query,
    generate_sample_query,
)


@dataclass
class TableProfile:
    """Complete profile of a table."""

    table_info: TableInfo
    columns: list[ColumnInfo]
    column_stats: list[ColumnStatistics]
    row_count: int
    size_bytes: int | None
    sample_data: pd.DataFrame | None = None


class TableProfiler:
    """Orchestrates table profiling operations."""

    def __init__(self, client: BigQueryClient | None = None):
        """Initialize the profiler."""
        self.client = client or BigQueryClient()
        self.settings = get_settings()

    def profile_table(
        self,
        dataset_id: str,
        table_id: str,
        include_samples: bool = True,
        include_top_values: bool = True,
        sample_rows: int = 5,
    ) -> TableProfile:
        """
        Generate a complete profile for a table.

        Args:
            dataset_id: The dataset containing the table
            table_id: The table to profile
            include_samples: Whether to include sample values
            include_top_values: Whether to include top frequent values
            sample_rows: Number of sample rows to include

        Returns:
            TableProfile with all statistics
        """
        try:
            # Get table metadata
            table_info = self.client.get_table_info(dataset_id, table_id)
            columns = self.client.get_table_schema(dataset_id, table_id)

            full_table_name = f"{self.client.project_id}.{dataset_id}.{table_id}"

            # Determine if we should use approximate functions
            use_approx = (
                table_info.num_rows
                and table_info.num_rows > self.settings.large_table_threshold
            )

            # Compute column statistics
            column_stats = self._compute_all_column_stats(
                full_table_name,
                columns,
                table_info.num_rows or 0,
                include_top_values,
                use_approx,
            )

            # Get sample data
            sample_data = None
            if include_samples:
                sample_data = self._get_sample_data(full_table_name, sample_rows)

            return TableProfile(
                table_info=table_info,
                columns=columns,
                column_stats=column_stats,
                row_count=table_info.num_rows or 0,
                size_bytes=table_info.num_bytes,
                sample_data=sample_data,
            )

        except Exception as e:
            raise ProfilerError(
                f"Failed to profile table: {str(e)}",
                table=f"{dataset_id}.{table_id}",
            )

    def _compute_all_column_stats(
        self,
        table_name: str,
        columns: list[ColumnInfo],
        total_rows: int,
        include_top_values: bool,
        use_approx: bool,
    ) -> list[ColumnStatistics]:
        """Compute statistics for all columns in a single query."""
        if not columns:
            return []

        # Build the statistics query
        stat_expressions = []
        for col in columns:
            expr = compute_column_stats(col.name, col.data_type)
            stat_expressions.append(expr)

        stats_sql = f"""
        SELECT
            COUNT(*) AS __total_count,
            {','.join(stat_expressions)}
        FROM `{table_name}`
        """

        # Execute statistics query
        stats_df = self.client.execute_query(stats_sql, validate=False)

        if stats_df.empty:
            return []

        stats_row = stats_df.iloc[0]
        total_count = int(stats_row.get("__total_count", total_rows))

        # Build ColumnStatistics objects
        column_stats = []
        for col in columns:
            col_stat = self._extract_column_stats(col, stats_row, total_count)

            # Get top values if requested
            if include_top_values:
                col_stat.top_values = self._get_top_values(
                    table_name, col.name, use_approx
                )

            column_stats.append(col_stat)

        return column_stats

    def _extract_column_stats(
        self,
        col: ColumnInfo,
        stats_row: pd.Series,
        total_count: int,
    ) -> ColumnStatistics:
        """Extract column statistics from the query result."""
        name = col.name

        # Get basic stats
        count = int(stats_row.get(f"{name}__count", 0))
        null_count = int(stats_row.get(f"{name}__null_count", 0))
        distinct_count = int(stats_row.get(f"{name}__distinct", 0))

        # Calculate percentages
        null_pct = (null_count / total_count * 100) if total_count > 0 else 0
        distinct_pct = (distinct_count / total_count * 100) if total_count > 0 else 0

        col_stat = ColumnStatistics(
            column_name=name,
            data_type=col.data_type,
            mode=col.mode,
            total_count=total_count,
            null_count=null_count,
            null_percentage=round(null_pct, 2),
            distinct_count=distinct_count,
            distinct_percentage=round(distinct_pct, 2),
        )

        # Type-specific stats
        numeric_types = {"INT64", "INTEGER", "FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "DECIMAL"}
        string_types = {"STRING", "BYTES"}

        if col.data_type.upper() in numeric_types:
            col_stat.min_value = self._safe_get(stats_row, f"{name}__min")
            col_stat.max_value = self._safe_get(stats_row, f"{name}__max")
            col_stat.avg_value = self._safe_get(stats_row, f"{name}__avg")
            col_stat.std_value = self._safe_get(stats_row, f"{name}__std")

        elif col.data_type.upper() in string_types:
            col_stat.min_length = self._safe_get(stats_row, f"{name}__min_len")
            col_stat.max_length = self._safe_get(stats_row, f"{name}__max_len")
            col_stat.avg_length = self._safe_get(stats_row, f"{name}__avg_len")

        else:
            # Date/other types
            col_stat.min_value = self._safe_get(stats_row, f"{name}__min")
            col_stat.max_value = self._safe_get(stats_row, f"{name}__max")

        return col_stat

    def _safe_get(self, row: pd.Series, key: str) -> Any:
        """Safely get a value from a pandas Series."""
        value = row.get(key)
        if pd.isna(value):
            return None
        return value

    def _get_top_values(
        self,
        table_name: str,
        column_name: str,
        use_approx: bool,
        top_n: int = 10,
    ) -> list[tuple[Any, int]]:
        """Get the top N most frequent values for a column."""
        try:
            query = generate_approx_top_values_query(table_name, column_name, top_n)
            df = self.client.execute_query(query, validate=False)

            if df.empty:
                return []

            return [(row["value"], int(row["count"])) for _, row in df.iterrows()]

        except Exception:
            # Fallback for types that don't support APPROX_TOP_COUNT
            return []

    def _get_sample_data(self, table_name: str, limit: int = 5) -> pd.DataFrame:
        """Get sample rows from the table."""
        query = f"SELECT * FROM `{table_name}` LIMIT {limit}"
        return self.client.execute_query(query, validate=False)

    def get_quick_stats(self, dataset_id: str, table_id: str) -> dict[str, Any]:
        """Get quick overview statistics without full profiling."""
        table_info = self.client.get_table_info(dataset_id, table_id)
        columns = self.client.get_table_schema(dataset_id, table_id)

        return {
            "table": table_info.full_name,
            "row_count": table_info.num_rows,
            "size_bytes": table_info.num_bytes,
            "size_mb": round(table_info.num_bytes / (1024 * 1024), 2) if table_info.num_bytes else None,
            "column_count": len(columns),
            "columns": [
                {"name": c.name, "type": c.data_type, "mode": c.mode}
                for c in columns
            ],
            "created": table_info.created,
            "modified": table_info.modified,
            "description": table_info.description,
        }
