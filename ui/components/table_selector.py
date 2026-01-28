"""Table and dataset selector components."""
import streamlit as st

from core.bigquery_client import BigQueryClient
from ui.session_manager import SessionManager


def render_dataset_selector(
    bq_client: BigQueryClient,
    key_prefix: str = "",
) -> str | None:
    """
    Render a dataset selector dropdown.

    Args:
        bq_client: BigQuery client
        key_prefix: Prefix for session state keys

    Returns:
        Selected dataset ID or None
    """
    try:
        datasets = bq_client.list_datasets()
    except Exception as e:
        st.error(f"Failed to list datasets: {str(e)}")
        return None

    if not datasets:
        st.warning("No datasets found in project.")
        return None

    # Get current selection
    current = SessionManager.get_selected_dataset()
    default_index = 0
    if current and current in datasets:
        default_index = datasets.index(current)

    selected = st.selectbox(
        "Select Dataset",
        options=datasets,
        index=default_index,
        key=f"{key_prefix}_dataset_selector",
    )

    if selected != current:
        SessionManager.set_selected_dataset(selected)

    return selected


def render_table_selector(
    bq_client: BigQueryClient,
    key_prefix: str = "",
) -> tuple[str | None, str | None]:
    """
    Render dataset and table selector dropdowns.

    Args:
        bq_client: BigQuery client
        key_prefix: Prefix for session state keys

    Returns:
        Tuple of (dataset_id, table_id) or (None, None)
    """
    # Dataset selection
    dataset = render_dataset_selector(bq_client, key_prefix)

    if not dataset:
        return None, None

    # Table selection
    try:
        tables = bq_client.list_tables(dataset)
    except Exception as e:
        st.error(f"Failed to list tables: {str(e)}")
        return dataset, None

    if not tables:
        st.warning(f"No tables found in dataset '{dataset}'.")
        return dataset, None

    table_names = [t.table for t in tables]

    # Get current selection
    current_table = SessionManager.get_selected_table()
    default_index = 0
    if current_table and current_table in table_names:
        default_index = table_names.index(current_table)

    selected_table = st.selectbox(
        "Select Table",
        options=table_names,
        index=default_index,
        key=f"{key_prefix}_table_selector",
        format_func=lambda t: _format_table_option(t, tables),
    )

    if selected_table != current_table:
        SessionManager.set_selected_table(selected_table)

    return dataset, selected_table


def _format_table_option(table_name: str, tables: list) -> str:
    """Format table name with row count for display."""
    for t in tables:
        if t.table == table_name:
            if t.num_rows:
                return f"{table_name} ({t.num_rows:,} rows)"
            return table_name
    return table_name


def render_table_multiselect(
    bq_client: BigQueryClient,
    dataset: str,
    key_prefix: str = "",
    default_all: bool = False,
) -> list[str]:
    """
    Render a multi-select for tables.

    Args:
        bq_client: BigQuery client
        dataset: Dataset ID
        key_prefix: Prefix for session state keys
        default_all: Whether to select all tables by default

    Returns:
        List of selected table names
    """
    try:
        tables = bq_client.list_tables(dataset)
    except Exception as e:
        st.error(f"Failed to list tables: {str(e)}")
        return []

    table_names = [t.table for t in tables]
    default = table_names if default_all else []

    selected = st.multiselect(
        "Select Tables",
        options=table_names,
        default=default,
        key=f"{key_prefix}_table_multiselect",
    )

    return selected
