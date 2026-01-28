"""Tests for the relationships module."""
import pytest
from unittest.mock import Mock

from relationships.column_matcher import ColumnMatcher, ColumnMatch
from relationships.detector import RelationshipDetector, Relationship
from relationships.graph_builder import RelationshipGraphBuilder
from core.bigquery_client import ColumnInfo


class TestColumnMatcher:
    """Tests for ColumnMatcher class."""

    @pytest.fixture
    def matcher(self):
        """Create a ColumnMatcher instance."""
        return ColumnMatcher(confidence_threshold=0.5)

    def test_exact_name_match(self, matcher):
        """Test matching columns with exact same name."""
        source_cols = [ColumnInfo(name="user_id", data_type="INT64", mode="NULLABLE", description=None)]
        target_cols = [ColumnInfo(name="user_id", data_type="INT64", mode="NULLABLE", description=None)]

        matches = matcher.find_matches("orders", source_cols, "users", target_cols)

        assert len(matches) == 1
        assert matches[0].confidence == 0.9
        assert matches[0].match_type == "exact"

    def test_fk_pattern_match(self, matcher):
        """Test matching FK pattern (user_id -> id)."""
        source_cols = [ColumnInfo(name="user_id", data_type="INT64", mode="NULLABLE", description=None)]
        target_cols = [ColumnInfo(name="id", data_type="INT64", mode="REQUIRED", description=None)]

        matches = matcher.find_matches("orders", source_cols, "users", target_cols)

        assert len(matches) == 1
        assert matches[0].confidence >= 0.7
        assert "pattern" in matches[0].match_type or "table_reference" in matches[0].match_type

    def test_type_incompatibility(self, matcher):
        """Test that incompatible types don't match."""
        source_cols = [ColumnInfo(name="user_id", data_type="STRING", mode="NULLABLE", description=None)]
        target_cols = [ColumnInfo(name="user_id", data_type="INT64", mode="NULLABLE", description=None)]

        matches = matcher.find_matches("orders", source_cols, "users", target_cols)

        # Should not match due to type incompatibility
        assert len(matches) == 0

    def test_confidence_threshold(self):
        """Test that matches below threshold are filtered."""
        # Use high threshold
        matcher = ColumnMatcher(confidence_threshold=0.95)

        source_cols = [ColumnInfo(name="customer_id", data_type="INT64", mode="NULLABLE", description=None)]
        target_cols = [ColumnInfo(name="id", data_type="INT64", mode="REQUIRED", description=None)]

        matches = matcher.find_matches("orders", source_cols, "customers", target_cols)

        # Pattern match should be below 0.95 threshold
        assert len(matches) == 0

    def test_table_name_matching_plural(self, matcher):
        """Test table name matching with plurals."""
        assert matcher._table_names_match("user", "users") is True
        assert matcher._table_names_match("order", "orders") is True
        assert matcher._table_names_match("category", "categories") is True

    def test_type_compatibility(self, matcher):
        """Test type compatibility checks."""
        assert matcher._types_compatible("INT64", "INT64") is True
        assert matcher._types_compatible("INT64", "INTEGER") is True
        assert matcher._types_compatible("STRING", "INT64") is False
        assert matcher._types_compatible("DATE", "TIMESTAMP") is True


class TestRelationshipDetector:
    """Tests for RelationshipDetector class."""

    @pytest.fixture
    def mock_bq_client(self):
        """Create a mock BigQuery client."""
        client = Mock()
        client.project_id = "test-project"

        # Mock list_tables
        client.list_tables.return_value = [
            Mock(table="users"),
            Mock(table="orders"),
            Mock(table="products"),
        ]

        # Mock schemas
        def get_schema(dataset, table):
            schemas = {
                "users": [
                    ColumnInfo(name="id", data_type="INT64", mode="REQUIRED", description=None),
                    ColumnInfo(name="name", data_type="STRING", mode="NULLABLE", description=None),
                ],
                "orders": [
                    ColumnInfo(name="id", data_type="INT64", mode="REQUIRED", description=None),
                    ColumnInfo(name="user_id", data_type="INT64", mode="NULLABLE", description=None),
                    ColumnInfo(name="product_id", data_type="INT64", mode="NULLABLE", description=None),
                ],
                "products": [
                    ColumnInfo(name="id", data_type="INT64", mode="REQUIRED", description=None),
                    ColumnInfo(name="name", data_type="STRING", mode="NULLABLE", description=None),
                ],
            }
            return schemas.get(table, [])

        client.get_table_schema.side_effect = get_schema

        # Mock execute_query to fail for FK query (BigQuery doesn't have FKs)
        client.execute_query.side_effect = Exception("Not supported")

        return client

    def test_detect_relationships(self, mock_bq_client):
        """Test detecting relationships."""
        detector = RelationshipDetector(mock_bq_client)
        relationships = detector.detect_relationships("test_dataset")

        # Should find orders.user_id -> users.id and orders.product_id -> products.id
        assert len(relationships) >= 2

    def test_relationship_details(self, mock_bq_client):
        """Test relationship details are correct."""
        detector = RelationshipDetector(mock_bq_client)
        relationships = detector.detect_relationships("test_dataset")

        # Find user relationship
        user_rel = next(
            (r for r in relationships if "user" in r.source_column.lower() or "user" in r.target_table.lower()),
            None
        )

        assert user_rel is not None
        assert user_rel.confidence > 0.5
        assert len(user_rel.evidence) > 0

    def test_merge_duplicate_relationships(self, mock_bq_client):
        """Test that duplicate relationships are merged."""
        detector = RelationshipDetector(mock_bq_client)

        # Create duplicate relationships
        rels = [
            Relationship("orders", "user_id", "users", "id", "inferred", 0.7, ["match1"]),
            Relationship("orders", "user_id", "users", "id", "inferred", 0.8, ["match2"]),
        ]

        merged = detector._merge_relationships(rels)

        # Should be merged into one
        assert len(merged) == 1
        assert merged[0].confidence == 0.8  # Keep higher confidence
        assert len(merged[0].evidence) == 2  # Combine evidence


class TestRelationshipGraphBuilder:
    """Tests for RelationshipGraphBuilder class."""

    @pytest.fixture
    def sample_relationships(self):
        """Create sample relationships."""
        return [
            Relationship("orders", "user_id", "users", "id", "inferred", 0.9, ["FK pattern"]),
            Relationship("orders", "product_id", "products", "id", "inferred", 0.8, ["FK pattern"]),
            Relationship("reviews", "user_id", "users", "id", "inferred", 0.85, ["FK pattern"]),
        ]

    def test_build_graph(self, sample_relationships):
        """Test building a graph from relationships."""
        builder = RelationshipGraphBuilder(sample_relationships)
        graph = builder.get_networkx_graph()

        # Check nodes
        assert "users" in graph.nodes()
        assert "orders" in graph.nodes()
        assert "products" in graph.nodes()
        assert "reviews" in graph.nodes()

        # Check edges
        assert graph.number_of_edges() == 3

    def test_get_node_info(self, sample_relationships):
        """Test getting node information."""
        builder = RelationshipGraphBuilder(sample_relationships)
        nodes = builder.get_node_info()

        # Users should have 2 incoming connections
        users_info = next(n for n in nodes if n["table"] == "users")
        assert users_info["in_degree"] == 2

    def test_get_edge_info(self, sample_relationships):
        """Test getting edge information."""
        builder = RelationshipGraphBuilder(sample_relationships)
        edges = builder.get_edge_info()

        assert len(edges) == 3

        # Check edge details
        order_user_edge = next(
            e for e in edges if e["source_table"] == "orders" and e["target_table"] == "users"
        )
        assert order_user_edge["confidence"] == 0.9

    def test_get_summary(self, sample_relationships):
        """Test getting graph summary."""
        builder = RelationshipGraphBuilder(sample_relationships)
        summary = builder.get_summary()

        assert summary["total_tables"] == 4
        assert summary["total_relationships"] == 3
        assert "users" in summary["hub_tables"]  # Most connections

    def test_empty_relationships(self):
        """Test handling empty relationships."""
        builder = RelationshipGraphBuilder([])
        summary = builder.get_summary()

        assert summary["total_tables"] == 0
        assert summary["total_relationships"] == 0

    def test_create_plotly_figure(self, sample_relationships):
        """Test creating Plotly figure."""
        builder = RelationshipGraphBuilder(sample_relationships)
        fig = builder.create_plotly_figure()

        # Should create a valid figure
        assert fig is not None
        assert hasattr(fig, "data")
        assert hasattr(fig, "layout")
