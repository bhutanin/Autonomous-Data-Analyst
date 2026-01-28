"""Tests for SQL validator."""
import pytest
from core.sql_validator import SQLValidator, SQLValidationError


class TestSQLValidator:
    """Tests for SQLValidator class."""

    def test_valid_select_query(self):
        """Test that valid SELECT queries pass validation."""
        valid_queries = [
            "SELECT * FROM table",
            "SELECT id, name FROM users WHERE id = 1",
            "SELECT COUNT(*) FROM orders GROUP BY customer_id",
            "SELECT a.*, b.name FROM table_a a JOIN table_b b ON a.id = b.a_id",
            "WITH cte AS (SELECT * FROM table) SELECT * FROM cte",
            "SELECT * FROM `project.dataset.table`",
            "SELECT DISTINCT category FROM products",
            "SELECT * FROM orders ORDER BY created_at DESC LIMIT 10",
        ]

        for query in valid_queries:
            result = SQLValidator.validate(query)
            assert result is not None

    def test_invalid_insert_query(self):
        """Test that INSERT queries are blocked."""
        with pytest.raises(SQLValidationError) as exc_info:
            SQLValidator.validate("INSERT INTO users (name) VALUES ('test')")
        assert "INSERT" in str(exc_info.value).upper()

    def test_invalid_update_query(self):
        """Test that UPDATE queries are blocked."""
        with pytest.raises(SQLValidationError) as exc_info:
            SQLValidator.validate("UPDATE users SET name = 'test' WHERE id = 1")
        assert "UPDATE" in str(exc_info.value).upper()

    def test_invalid_delete_query(self):
        """Test that DELETE queries are blocked."""
        with pytest.raises(SQLValidationError) as exc_info:
            SQLValidator.validate("DELETE FROM users WHERE id = 1")
        assert "DELETE" in str(exc_info.value).upper()

    def test_invalid_drop_query(self):
        """Test that DROP queries are blocked."""
        with pytest.raises(SQLValidationError) as exc_info:
            SQLValidator.validate("DROP TABLE users")
        assert "DROP" in str(exc_info.value).upper()

    def test_invalid_create_query(self):
        """Test that CREATE queries are blocked."""
        with pytest.raises(SQLValidationError) as exc_info:
            SQLValidator.validate("CREATE TABLE test (id INT)")
        assert "CREATE" in str(exc_info.value).upper()

    def test_invalid_alter_query(self):
        """Test that ALTER queries are blocked."""
        with pytest.raises(SQLValidationError) as exc_info:
            SQLValidator.validate("ALTER TABLE users ADD COLUMN email STRING")
        assert "ALTER" in str(exc_info.value).upper()

    def test_invalid_truncate_query(self):
        """Test that TRUNCATE queries are blocked."""
        with pytest.raises(SQLValidationError) as exc_info:
            SQLValidator.validate("TRUNCATE TABLE users")
        assert "TRUNCATE" in str(exc_info.value).upper()

    def test_empty_query(self):
        """Test that empty queries are rejected."""
        with pytest.raises(SQLValidationError):
            SQLValidator.validate("")

        with pytest.raises(SQLValidationError):
            SQLValidator.validate("   ")

    def test_is_valid_helper(self):
        """Test the is_valid helper method."""
        assert SQLValidator.is_valid("SELECT * FROM table") is True
        assert SQLValidator.is_valid("DELETE FROM table") is False
        assert SQLValidator.is_valid("") is False

    def test_query_with_comments(self):
        """Test that queries with comments are handled correctly."""
        # Valid query with comment
        result = SQLValidator.validate("""
            -- This is a comment
            SELECT * FROM users
        """)
        assert result is not None

        # Blocked operation even with comment
        with pytest.raises(SQLValidationError):
            SQLValidator.validate("""
                -- Just selecting
                DELETE FROM users
            """)

    def test_select_into_blocked(self):
        """Test that SELECT INTO patterns are blocked."""
        with pytest.raises(SQLValidationError):
            SQLValidator.validate("SELECT * INTO new_table FROM old_table")

    def test_subquery_allowed(self):
        """Test that subqueries are allowed in SELECT."""
        result = SQLValidator.validate("""
            SELECT *
            FROM users
            WHERE id IN (SELECT user_id FROM orders)
        """)
        assert result is not None

    def test_union_allowed(self):
        """Test that UNION queries are allowed."""
        result = SQLValidator.validate("""
            SELECT id, name FROM users
            UNION ALL
            SELECT id, name FROM admins
        """)
        assert result is not None


class TestExtractTables:
    """Tests for table extraction from SQL."""

    def test_simple_table_extraction(self):
        """Test extracting tables from simple queries."""
        sql = "SELECT * FROM users"
        tables = SQLValidator.extract_tables(sql)
        assert "users" in tables

    def test_join_table_extraction(self):
        """Test extracting tables from JOIN queries."""
        sql = """
            SELECT *
            FROM users u
            JOIN orders o ON u.id = o.user_id
        """
        tables = SQLValidator.extract_tables(sql)
        assert "users" in tables or "u" in tables

    def test_qualified_table_name(self):
        """Test extracting qualified table names."""
        sql = "SELECT * FROM `project.dataset.table`"
        tables = SQLValidator.extract_tables(sql)
        assert len(tables) > 0
