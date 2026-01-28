"""Relationship graph visualization component."""
import streamlit as st

from relationships.graph_builder import RelationshipGraphBuilder


def render_relationship_graph(relationships: list) -> None:
    """
    Render an interactive relationship graph.

    Args:
        relationships: List of Relationship objects
    """
    if not relationships:
        st.info("No relationships to display.")
        return

    # Build graph
    builder = RelationshipGraphBuilder(relationships)

    # Create and display Plotly figure
    fig = builder.create_plotly_figure(
        title="Table Relationships",
        width=800,
        height=500,
    )

    st.plotly_chart(fig, use_container_width=True)

    # Display summary
    summary = builder.get_summary()

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("**Hub Tables** (most connections)")
        for table in summary["hub_tables"][:5]:
            degree = builder.get_networkx_graph().degree(table)
            st.write(f"- {table} ({degree} connections)")

    with col2:
        if summary["isolated_tables"]:
            st.markdown("**Isolated Tables** (no connections)")
            for table in summary["isolated_tables"]:
                st.write(f"- {table}")


def render_relationship_legend() -> None:
    """Render a legend explaining the graph colors."""
    st.markdown("""
    **Graph Legend:**
    - 🟢 **Green edges**: High confidence (≥90%)
    - 🔵 **Blue edges**: Medium confidence (70-90%)
    - 🟠 **Orange edges**: Lower confidence (<70%)
    - **Line thickness**: Proportional to confidence score
    """)
