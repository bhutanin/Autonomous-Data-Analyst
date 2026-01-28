"""Data profiling page."""
import streamlit as st
import pandas as pd

from core.bigquery_client import BigQueryClient
from profiler.table_profiler import TableProfiler, TableProfile
from ui.session_manager import SessionManager
from ui.components.table_selector import render_table_selector
from ui.components.profile_display import render_profile_display


def render_data_profiling_page(bq_client: BigQueryClient) -> None:
    """Render the data profiling page."""
    st.header("Data Profiling")
    st.markdown(
        "Generate comprehensive statistics for your BigQuery tables including "
        "null percentages, unique counts, value distributions, and more."
    )

    # Table selection
    dataset, table = render_table_selector(bq_client, key_prefix="profiling")

    if not dataset or not table:
        st.info("Select a dataset and table to begin profiling.")
        return

    # Check for cached profile
    table_key = f"{dataset}.{table}"
    cached_profile = SessionManager.get_cached_profile(table_key)

    col1, col2 = st.columns([3, 1])

    with col1:
        include_samples = st.checkbox("Include sample values", value=True)
        include_top_values = st.checkbox("Include top frequent values", value=True)

    with col2:
        if st.button("Run Profiling", type="primary", use_container_width=True):
            _run_profiling(bq_client, dataset, table, include_samples, include_top_values)

    # Display cached or new profile
    if cached_profile:
        st.divider()
        render_profile_display(cached_profile)
    elif SessionManager.get_cached_profile(table_key) is None:
        st.info("Click 'Run Profiling' to generate statistics for this table.")


def _run_profiling(
    bq_client: BigQueryClient,
    dataset: str,
    table: str,
    include_samples: bool,
    include_top_values: bool,
) -> None:
    """Run profiling and cache results."""
    table_key = f"{dataset}.{table}"

    with st.spinner(f"Profiling {table}..."):
        try:
            profiler = TableProfiler(bq_client)
            profile = profiler.profile_table(
                dataset_id=dataset,
                table_id=table,
                include_samples=include_samples,
                include_top_values=include_top_values,
            )
            SessionManager.set_cached_profile(table_key, profile)
            st.success("Profiling complete!")
            st.rerun()

        except Exception as e:
            st.error(f"Profiling failed: {str(e)}")
