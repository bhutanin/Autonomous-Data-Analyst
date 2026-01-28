"""Custom exceptions for the Data Analyst application."""


class DataAnalystError(Exception):
    """Base exception for all application errors."""

    pass


class QueryExecutionError(DataAnalystError):
    """Raised when a BigQuery query fails to execute."""

    def __init__(self, message: str, query: str | None = None, original_error: Exception | None = None):
        super().__init__(message)
        self.query = query
        self.original_error = original_error


class ProfilerError(DataAnalystError):
    """Raised when profiling a table fails."""

    def __init__(self, message: str, table: str | None = None):
        super().__init__(message)
        self.table = table


class RelationshipDetectionError(DataAnalystError):
    """Raised when relationship detection fails."""

    pass


class LLMError(DataAnalystError):
    """Raised when LLM operations fail."""

    def __init__(self, message: str, prompt: str | None = None):
        super().__init__(message)
        self.prompt = prompt


class SQLGenerationError(LLMError):
    """Raised when SQL generation fails after retries."""

    def __init__(self, message: str, attempts: int = 0, last_sql: str | None = None):
        super().__init__(message)
        self.attempts = attempts
        self.last_sql = last_sql
