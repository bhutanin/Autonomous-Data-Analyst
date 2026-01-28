"""SQL validation to enforce SELECT-only queries."""
import re
import sqlparse
from sqlparse.sql import Statement
from sqlparse.tokens import Keyword, DML


class SQLValidationError(Exception):
    """Raised when SQL validation fails."""

    def __init__(self, message: str, sql: str | None = None):
        super().__init__(message)
        self.sql = sql


class SQLValidator:
    """Validates SQL queries to ensure only SELECT statements are allowed."""

    # Dangerous keywords that indicate non-SELECT operations
    BLOCKED_KEYWORDS = {
        "INSERT",
        "UPDATE",
        "DELETE",
        "DROP",
        "CREATE",
        "ALTER",
        "TRUNCATE",
        "REPLACE",
        "MERGE",
        "GRANT",
        "REVOKE",
        "EXECUTE",
        "EXEC",
        "CALL",
    }

    # Patterns that might indicate dangerous operations
    BLOCKED_PATTERNS = [
        r"\bINTO\s+\w+",  # INSERT INTO or SELECT INTO
        r"\bDROP\s+(TABLE|DATABASE|SCHEMA|VIEW|INDEX)",
        r"\bCREATE\s+(TABLE|DATABASE|SCHEMA|VIEW|INDEX|FUNCTION|PROCEDURE)",
        r"\bALTER\s+(TABLE|DATABASE|SCHEMA)",
        r"\bTRUNCATE\s+TABLE",
        r"\bEXEC(UTE)?\s*\(",
    ]

    @classmethod
    def validate(cls, sql: str) -> str:
        """
        Validate that the SQL is a safe SELECT query.

        Args:
            sql: The SQL query to validate

        Returns:
            The cleaned SQL if valid

        Raises:
            SQLValidationError: If the SQL contains dangerous operations
        """
        if not sql or not sql.strip():
            raise SQLValidationError("Empty SQL query", sql)

        # Remove comments and normalize whitespace
        cleaned_sql = cls._clean_sql(sql)

        # Parse the SQL
        parsed = sqlparse.parse(cleaned_sql)

        if not parsed:
            raise SQLValidationError("Failed to parse SQL query", sql)

        # Check each statement
        for statement in parsed:
            cls._validate_statement(statement, sql)

        return cleaned_sql

    @classmethod
    def _clean_sql(cls, sql: str) -> str:
        """Remove comments and normalize the SQL."""
        # Remove SQL comments
        cleaned = sqlparse.format(
            sql,
            strip_comments=True,
            strip_whitespace=True,
        )
        return cleaned.strip()

    @classmethod
    def _validate_statement(cls, statement: Statement, original_sql: str) -> None:
        """Validate a single SQL statement."""
        # Get the statement type
        stmt_type = statement.get_type()

        # Allow SELECT and WITH (CTEs)
        if stmt_type not in ("SELECT", "UNKNOWN", None):
            raise SQLValidationError(
                f"Only SELECT queries are allowed. Found: {stmt_type}",
                original_sql,
            )

        # Get the first real token to check statement type
        first_token = None
        for token in statement.tokens:
            if token.ttype in (Keyword, DML) or (
                token.ttype is None and not token.is_whitespace
            ):
                first_token = token
                break

        if first_token:
            token_value = first_token.value.upper()

            # Check for blocked keywords at the start
            if token_value in cls.BLOCKED_KEYWORDS:
                raise SQLValidationError(
                    f"Dangerous operation detected: {token_value}",
                    original_sql,
                )

        # Check all tokens for blocked keywords
        sql_upper = statement.value.upper()
        for keyword in cls.BLOCKED_KEYWORDS:
            # Use word boundary matching
            pattern = rf"\b{keyword}\b"
            if re.search(pattern, sql_upper):
                # Allow DELETE in comments or strings (already stripped)
                # but still need to check context
                if keyword in ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE"):
                    raise SQLValidationError(
                        f"Dangerous operation detected: {keyword}",
                        original_sql,
                    )

        # Check for blocked patterns
        for pattern in cls.BLOCKED_PATTERNS:
            if re.search(pattern, sql_upper, re.IGNORECASE):
                raise SQLValidationError(
                    f"Potentially dangerous SQL pattern detected",
                    original_sql,
                )

    @classmethod
    def is_valid(cls, sql: str) -> bool:
        """
        Check if SQL is valid without raising an exception.

        Args:
            sql: The SQL query to check

        Returns:
            True if the SQL is a valid SELECT query, False otherwise
        """
        try:
            cls.validate(sql)
            return True
        except SQLValidationError:
            return False

    @classmethod
    def extract_tables(cls, sql: str) -> list[str]:
        """
        Extract table names from a SQL query.

        Args:
            sql: The SQL query

        Returns:
            List of table names referenced in the query
        """
        tables = []
        parsed = sqlparse.parse(sql)

        for statement in parsed:
            tables.extend(cls._extract_tables_from_statement(statement))

        return list(set(tables))

    @classmethod
    def _extract_tables_from_statement(cls, statement: Statement) -> list[str]:
        """Extract tables from a single statement."""
        tables = []
        from_seen = False

        for token in statement.tokens:
            if token.is_whitespace:
                continue

            # Check for FROM or JOIN keywords
            if token.ttype is Keyword:
                word = token.value.upper()
                if word in ("FROM", "JOIN", "INNER", "LEFT", "RIGHT", "FULL", "CROSS"):
                    from_seen = True
                elif word in ("WHERE", "GROUP", "ORDER", "HAVING", "LIMIT", "UNION"):
                    from_seen = False

            # Capture table names after FROM/JOIN
            elif from_seen and token.ttype is None:
                # This could be a table name or identifier
                table_name = token.value.strip()
                # Remove aliases
                if " " in table_name:
                    table_name = table_name.split()[0]
                if table_name and not table_name.upper().startswith(("ON", "AND", "OR")):
                    tables.append(table_name)
                from_seen = False

        return tables
