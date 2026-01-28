"""Relationships detection page."""
import streamlit as st

from core.bigquery_client import BigQueryClient
from relationships.detector import RelationshipDetector
from relationships.graph_builder import RelationshipGraphBuilder
from ui.session_manager import SessionManager
from ui.components.table_selector import render_dataset_selector
from ui.components.relationship_graph import render_relationship_graph


def render_relationships_page(bq_client: BigQueryClient) -> None:
    """Render the relationships detection page."""
    st.header("Relationship Detection")
    st.markdown(
        "Automatically detect relationships between tables based on "
        "column naming patterns, foreign key conventions, and type compatibility."
    )

    # Dataset selection
    dataset = render_dataset_selector(bq_client, key_prefix="relationships")

    if not dataset:
        st.info("Select a dataset to detect relationships.")
        return

    # Check for cached relationships
    cached_relationships = SessionManager.get_cached_relationships(dataset)

    col1, col2 = st.columns([3, 1])

    with col1:
        # Get tables for selection
        tables = bq_client.list_tables(dataset)
        table_names = [t.table for t in tables]

        selected_tables = st.multiselect(
            "Select tables (leave empty for all)",
            options=table_names,
            default=[],
            help="Choose specific tables or leave empty to analyze all tables",
        )

    with col2:
        if st.button("Detect Relationships", type="primary", use_container_width=True):
            _run_detection(bq_client, dataset, selected_tables or None)

    # Display results
    if cached_relationships is not None:
        st.divider()
        _display_relationships(cached_relationships, dataset)


def _run_detection(
    bq_client: BigQueryClient,
    dataset: str,
    table_ids: list[str] | None,
) -> None:
    """Run relationship detection and cache results."""
    with st.spinner("Detecting relationships..."):
        try:
            detector = RelationshipDetector(bq_client)
            relationships = detector.detect_relationships(dataset, table_ids)
            SessionManager.set_cached_relationships(dataset, relationships)
            st.success(f"Found {len(relationships)} relationship(s)!")
            st.rerun()

        except Exception as e:
            st.error(f"Detection failed: {str(e)}")


def _display_relationships(relationships: list, dataset: str) -> None:
    """Display detected relationships."""
    if not relationships:
        st.warning("No relationships detected between tables.")
        st.markdown(
            """
            **Possible reasons:**
            - Tables don't follow standard foreign key naming conventions
            - No overlapping column types between tables
            - Tables are unrelated
            """
        )
        return

    # Summary
    st.subheader("Relationship Summary")

    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Relationships", len(relationships))

    high_conf = sum(1 for r in relationships if r.confidence >= 0.8)
    with col2:
        st.metric("High Confidence", high_conf)

    tables = set()
    for r in relationships:
        tables.add(r.source_table)
        tables.add(r.target_table)
    with col3:
        st.metric("Tables Involved", len(tables))

    # Graph visualization
    st.subheader("Relationship Graph")
    render_relationship_graph(relationships)

    # Detailed table
    st.subheader("Relationship Details")
    _display_relationship_table(relationships)


def _display_relationship_table(relationships: list) -> None:
    """Display relationships in a table format."""
    data = []
    for rel in relationships:
        data.append({
            "Source Table": rel.source_table,
            "Source Column": rel.source_column,
            "Target Table": rel.target_table,
            "Target Column": rel.target_column,
            "Type": rel.relationship_type,
            "Confidence": f"{rel.confidence:.0%}",
        })

    st.dataframe(
        data,
        use_container_width=True,
        hide_index=True,
    )

    # Expandable evidence details
    with st.expander("View Evidence Details"):
        for i, rel in enumerate(relationships):
            st.markdown(f"**{rel.source_table}.{rel.source_column} → {rel.target_table}.{rel.target_column}**")
            for evidence in rel.evidence:
                st.markdown(f"  - {evidence}")
            if i < len(relationships) - 1:
                st.divider()
