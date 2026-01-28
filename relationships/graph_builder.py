"""Relationship graph construction and visualization."""
from dataclasses import dataclass
from typing import Any

import networkx as nx
import plotly.graph_objects as go

from .detector import Relationship


@dataclass
class GraphNode:
    """A node in the relationship graph."""

    table_name: str
    column_count: int
    row_count: int | None


class RelationshipGraphBuilder:
    """Builds and visualizes relationship graphs."""

    def __init__(self, relationships: list[Relationship]):
        """Initialize with relationships."""
        self.relationships = relationships
        self.graph = nx.DiGraph()
        self._build_graph()

    def _build_graph(self) -> None:
        """Build a NetworkX graph from relationships."""
        # Add nodes and edges
        for rel in self.relationships:
            # Add source node
            if rel.source_table not in self.graph:
                self.graph.add_node(rel.source_table)

            # Add target node
            if rel.target_table not in self.graph:
                self.graph.add_node(rel.target_table)

            # Add edge with relationship data
            self.graph.add_edge(
                rel.source_table,
                rel.target_table,
                source_column=rel.source_column,
                target_column=rel.target_column,
                relationship_type=rel.relationship_type,
                confidence=rel.confidence,
                evidence=rel.evidence,
            )

    def get_networkx_graph(self) -> nx.DiGraph:
        """Get the NetworkX graph object."""
        return self.graph

    def get_node_info(self) -> list[dict[str, Any]]:
        """Get information about all nodes."""
        return [
            {
                "table": node,
                "in_degree": self.graph.in_degree(node),
                "out_degree": self.graph.out_degree(node),
                "total_connections": self.graph.degree(node),
            }
            for node in self.graph.nodes()
        ]

    def get_edge_info(self) -> list[dict[str, Any]]:
        """Get information about all edges."""
        edges = []
        for source, target, data in self.graph.edges(data=True):
            edges.append({
                "source_table": source,
                "target_table": target,
                "source_column": data.get("source_column"),
                "target_column": data.get("target_column"),
                "relationship_type": data.get("relationship_type"),
                "confidence": data.get("confidence", 0),
            })
        return edges

    def create_plotly_figure(
        self,
        title: str = "Table Relationships",
        width: int = 800,
        height: int = 600,
    ) -> go.Figure:
        """
        Create an interactive Plotly visualization of the relationship graph.

        Args:
            title: Chart title
            width: Figure width in pixels
            height: Figure height in pixels

        Returns:
            Plotly Figure object
        """
        if not self.graph.nodes():
            # Return empty figure
            fig = go.Figure()
            fig.add_annotation(
                text="No relationships detected",
                xref="paper",
                yref="paper",
                x=0.5,
                y=0.5,
                showarrow=False,
                font=dict(size=20),
            )
            fig.update_layout(
                title=title,
                width=width,
                height=height,
            )
            return fig

        # Get layout positions using spring layout
        pos = nx.spring_layout(self.graph, k=2, iterations=50, seed=42)

        # Create edge traces
        edge_traces = []
        edge_annotations = []

        for edge in self.graph.edges(data=True):
            source, target, data = edge
            x0, y0 = pos[source]
            x1, y1 = pos[target]

            # Color based on confidence
            confidence = data.get("confidence", 0.5)
            if confidence >= 0.9:
                color = "green"
            elif confidence >= 0.7:
                color = "blue"
            else:
                color = "orange"

            # Line width based on confidence
            width_val = 1 + confidence * 3

            edge_trace = go.Scatter(
                x=[x0, x1, None],
                y=[y0, y1, None],
                mode="lines",
                line=dict(width=width_val, color=color),
                hoverinfo="none",
            )
            edge_traces.append(edge_trace)

            # Add annotation for edge label
            mid_x = (x0 + x1) / 2
            mid_y = (y0 + y1) / 2
            label = f"{data.get('source_column')} -> {data.get('target_column')}"

            edge_annotations.append(
                dict(
                    x=mid_x,
                    y=mid_y,
                    text=label,
                    showarrow=False,
                    font=dict(size=9, color="gray"),
                    bgcolor="white",
                    opacity=0.8,
                )
            )

        # Create node trace
        node_x = []
        node_y = []
        node_text = []
        node_hover = []

        for node in self.graph.nodes():
            x, y = pos[node]
            node_x.append(x)
            node_y.append(y)
            node_text.append(node)

            # Build hover text
            in_deg = self.graph.in_degree(node)
            out_deg = self.graph.out_degree(node)
            hover = f"<b>{node}</b><br>Incoming: {in_deg}<br>Outgoing: {out_deg}"
            node_hover.append(hover)

        node_trace = go.Scatter(
            x=node_x,
            y=node_y,
            mode="markers+text",
            hoverinfo="text",
            text=node_text,
            textposition="top center",
            hovertext=node_hover,
            marker=dict(
                size=30,
                color="lightblue",
                line=dict(width=2, color="darkblue"),
            ),
        )

        # Create figure
        fig = go.Figure(
            data=edge_traces + [node_trace],
            layout=go.Layout(
                title=title,
                showlegend=False,
                hovermode="closest",
                width=width,
                height=height,
                xaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                yaxis=dict(showgrid=False, zeroline=False, showticklabels=False),
                annotations=edge_annotations,
            ),
        )

        # Add legend for confidence colors
        fig.add_annotation(
            text="Confidence: <span style='color:green'>High (≥0.9)</span> | <span style='color:blue'>Medium (≥0.7)</span> | <span style='color:orange'>Low (<0.7)</span>",
            xref="paper",
            yref="paper",
            x=0.5,
            y=-0.05,
            showarrow=False,
            font=dict(size=10),
        )

        return fig

    def get_summary(self) -> dict[str, Any]:
        """Get a summary of the relationship graph."""
        return {
            "total_tables": self.graph.number_of_nodes(),
            "total_relationships": self.graph.number_of_edges(),
            "tables": list(self.graph.nodes()),
            "isolated_tables": [
                node for node in self.graph.nodes()
                if self.graph.degree(node) == 0
            ],
            "hub_tables": sorted(
                self.graph.nodes(),
                key=lambda n: self.graph.degree(n),
                reverse=True,
            )[:5],
        }
