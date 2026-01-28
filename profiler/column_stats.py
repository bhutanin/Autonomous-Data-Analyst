"""Column-level statistics computation."""
from dataclasses import dataclass
from typing import Any


@dataclass
class ColumnStatistics:
    """Statistics for a single column."""

    column_name: str
    data_type: str
    mode: str

    # Basic stats
    total_count: int
    null_count: int
    null_percentage: float
    distinct_count: int
    distinct_percentage: float

    # Value stats (type-dependent)
    min_value: Any | None = None
    max_value: Any | None = None
    avg_value: float | None = None
    std_value: float | None = None

    # String stats
    min_length: int | None = None
    max_length: int | None = None
    avg_length: float | None = None

    # Sample values
    sample_values: list[Any] | None = None
    top_values: list[tuple[Any, int]] | None = None


def compute_column_stats(column_name: str, data_type: str) -> str:
    """
    Generate SQL expressions for computing column statistics.

    Args:
        column_name: Name of the column
        data_type: BigQuery data type

    Returns:
        SQL expression fragments for the column
    """
    safe_col = f"`{column_name}`"
    stats = []

    # Basic counts
    stats.append(f"COUNT({safe_col}) AS `{column_name}__count`")
    stats.append(f"COUNTIF({safe_col} IS NULL) AS `{column_name}__null_count`")
    stats.append(f"APPROX_COUNT_DISTINCT({safe_col}) AS `{column_name}__distinct`")

    # Type-specific stats
    numeric_types = {"INT64", "INTEGER", "FLOAT64", "FLOAT", "NUMERIC", "BIGNUMERIC", "DECIMAL"}
    string_types = {"STRING", "BYTES"}
    date_types = {"DATE", "DATETIME", "TIMESTAMP", "TIME"}

    if data_type.upper() in numeric_types:
        stats.append(f"MIN({safe_col}) AS `{column_name}__min`")
        stats.append(f"MAX({safe_col}) AS `{column_name}__max`")
        stats.append(f"AVG({safe_col}) AS `{column_name}__avg`")
        stats.append(f"STDDEV({safe_col}) AS `{column_name}__std`")

    elif data_type.upper() in string_types:
        stats.append(f"MIN(LENGTH({safe_col})) AS `{column_name}__min_len`")
        stats.append(f"MAX(LENGTH({safe_col})) AS `{column_name}__max_len`")
        stats.append(f"AVG(LENGTH({safe_col})) AS `{column_name}__avg_len`")

    elif data_type.upper() in date_types:
        stats.append(f"MIN({safe_col}) AS `{column_name}__min`")
        stats.append(f"MAX({safe_col}) AS `{column_name}__max`")

    return ",\n    ".join(stats)


def generate_sample_query(table_name: str, column_name: str, limit: int = 5) -> str:
    """Generate a query to get sample non-null values."""
    safe_col = f"`{column_name}`"
    return f"""
    SELECT DISTINCT {safe_col}
    FROM `{table_name}`
    WHERE {safe_col} IS NOT NULL
    LIMIT {limit}
    """


def generate_top_values_query(table_name: str, column_name: str, top_n: int = 10) -> str:
    """Generate a query to get top frequent values."""
    safe_col = f"`{column_name}`"
    return f"""
    SELECT
        {safe_col} AS value,
        COUNT(*) AS count
    FROM `{table_name}`
    WHERE {safe_col} IS NOT NULL
    GROUP BY {safe_col}
    ORDER BY count DESC
    LIMIT {top_n}
    """


def generate_approx_top_values_query(table_name: str, column_name: str, top_n: int = 10) -> str:
    """Generate a query using APPROX_TOP_COUNT for large tables."""
    safe_col = f"`{column_name}`"
    return f"""
    SELECT
        value,
        count
    FROM UNNEST(
        (SELECT APPROX_TOP_COUNT({safe_col}, {top_n}) FROM `{table_name}`)
    )
    """
