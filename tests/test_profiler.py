"""Tests for the profiler module."""
import pytest
from unittest.mock import Mock, MagicMock, patch
import pandas as pd

from profiler.column_stats import (
    ColumnStatistics,
    compute_column_stats,
    generate_sample_query,
    generate_top_values_query,
    generate_approx_top_values_query,
)
from profiler.table_profiler import TableProfiler, TableProfile
from core.bigquery_client import TableInfo, ColumnInfo


class TestColumnStats:
    """Tests for column statistics functions."""

    def test_compute_column_stats_numeric(self):
        """Test SQL generation for numeric columns."""
        sql = compute_column_stats("price", "FLOAT64")

        assert "COUNT(`price`)" in sql
        assert "COUNTIF(`price` IS NULL)" in sql
        assert "APPROX_COUNT_DISTINCT(`price`)" in sql
        assert "MIN(`price`)" in sql
        assert "MAX(`price`)" in sql
        assert "AVG(`price`)" in sql
        assert "STDDEV(`price`)" in sql

    def test_compute_column_stats_string(self):
        """Test SQL generation for string columns."""
        sql = compute_column_stats("name", "STRING")

        assert "COUNT(`name`)" in sql
        assert "MIN(LENGTH(`name`))" in sql
        assert "MAX(LENGTH(`name`))" in sql
        assert "AVG(LENGTH(`name`))" in sql
        # Should not have numeric aggregates
        assert "STDDEV(`name`)" not in sql

    def test_compute_column_stats_date(self):
        """Test SQL generation for date columns."""
        sql = compute_column_stats("created_at", "TIMESTAMP")

        assert "COUNT(`created_at`)" in sql
        assert "MIN(`created_at`)" in sql
        assert "MAX(`created_at`)" in sql
        # Should not have string length stats
        assert "LENGTH(`created_at`)" not in sql

    def test_generate_sample_query(self):
        """Test sample query generation."""
        query = generate_sample_query("project.dataset.table", "name", limit=10)

        assert "SELECT DISTINCT" in query
        assert "`name`" in query
        assert "project.dataset.table" in query
        assert "LIMIT 10" in query
        assert "IS NOT NULL" in query

    def test_generate_top_values_query(self):
        """Test top values query generation."""
        query = generate_top_values_query("project.dataset.table", "category", top_n=5)

        assert "`category`" in query
        assert "COUNT(*)" in query
        assert "GROUP BY" in query
        assert "ORDER BY count DESC" in query
        assert "LIMIT 5" in query

    def test_generate_approx_top_values_query(self):
        """Test approximate top values query generation."""
        query = generate_approx_top_values_query("project.dataset.table", "status", top_n=10)

        assert "APPROX_TOP_COUNT" in query
        assert "`status`" in query
        assert "10" in query


class TestColumnStatistics:
    """Tests for ColumnStatistics dataclass."""

    def test_column_statistics_creation(self):
        """Test creating a ColumnStatistics object."""
        stats = ColumnStatistics(
            column_name="test_col",
            data_type="INT64",
            mode="NULLABLE",
            total_count=1000,
            null_count=50,
            null_percentage=5.0,
            distinct_count=100,
            distinct_percentage=10.0,
            min_value=1,
            max_value=500,
            avg_value=250.5,
        )

        assert stats.column_name == "test_col"
        assert stats.null_percentage == 5.0
        assert stats.distinct_count == 100


class TestTableProfiler:
    """Tests for TableProfiler class."""

    @pytest.fixture
    def mock_bq_client(self):
        """Create a mock BigQuery client."""
        client = Mock()
        client.project_id = "test-project"

        # Mock table info
        client.get_table_info.return_value = TableInfo(
            project="test-project",
            dataset="test_dataset",
            table="test_table",
            full_name="test-project.test_dataset.test_table",
            num_rows=1000,
            num_bytes=1024000,
            created="2024-01-01",
            modified="2024-01-15",
            description="Test table",
        )

        # Mock schema
        client.get_table_schema.return_value = [
            ColumnInfo(name="id", data_type="INT64", mode="REQUIRED", description=None),
            ColumnInfo(name="name", data_type="STRING", mode="NULLABLE", description=None),
        ]

        # Mock query execution
        def mock_execute(sql, validate=True):
            if "__total_count" in sql:
                return pd.DataFrame({
                    "__total_count": [1000],
                    "id__count": [1000],
                    "id__null_count": [0],
                    "id__distinct": [1000],
                    "id__min": [1],
                    "id__max": [1000],
                    "id__avg": [500.5],
                    "id__std": [288.6],
                    "name__count": [950],
                    "name__null_count": [50],
                    "name__distinct": [200],
                    "name__min_len": [2],
                    "name__max_len": [100],
                    "name__avg_len": [15.5],
                })
            elif "APPROX_TOP_COUNT" in sql:
                return pd.DataFrame({
                    "value": ["John", "Jane", "Bob"],
                    "count": [100, 80, 60],
                })
            else:
                return pd.DataFrame({"id": [1, 2], "name": ["Test1", "Test2"]})

        client.execute_query.side_effect = mock_execute
        return client

    def test_profile_table(self, mock_bq_client):
        """Test profiling a table."""
        profiler = TableProfiler(mock_bq_client)
        profile = profiler.profile_table("test_dataset", "test_table")

        assert isinstance(profile, TableProfile)
        assert profile.row_count == 1000
        assert len(profile.columns) == 2
        assert len(profile.column_stats) == 2

    def test_profile_table_column_stats(self, mock_bq_client):
        """Test that column stats are correctly extracted."""
        profiler = TableProfiler(mock_bq_client)
        profile = profiler.profile_table("test_dataset", "test_table")

        id_stats = next(s for s in profile.column_stats if s.column_name == "id")
        assert id_stats.null_count == 0
        assert id_stats.distinct_count == 1000
        assert id_stats.min_value == 1
        assert id_stats.max_value == 1000

        name_stats = next(s for s in profile.column_stats if s.column_name == "name")
        assert name_stats.null_count == 50
        assert name_stats.min_length == 2
        assert name_stats.max_length == 100

    def test_get_quick_stats(self, mock_bq_client):
        """Test getting quick stats."""
        profiler = TableProfiler(mock_bq_client)
        stats = profiler.get_quick_stats("test_dataset", "test_table")

        assert stats["row_count"] == 1000
        assert stats["column_count"] == 2
        assert "size_mb" in stats
