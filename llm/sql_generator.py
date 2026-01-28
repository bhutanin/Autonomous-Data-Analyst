"""Text-to-SQL generation orchestration."""
from dataclasses import dataclass
from typing import Any

from core.bigquery_client import BigQueryClient
from core.sql_validator import SQLValidator, SQLValidationError
from core.exceptions import SQLGenerationError, QueryExecutionError
from .gemini_client import GeminiClient, ChatMessage
from .prompt_templates import PromptTemplates
from .schema_context import SchemaContextBuilder


@dataclass
class SQLGenerationResult:
    """Result of SQL generation."""

    success: bool
    sql: str | None
    error: str | None
    attempts: int
    explanation: str | None = None


@dataclass
class QueryResult:
    """Result of query execution."""

    success: bool
    sql: str
    data: Any | None  # DataFrame
    row_count: int
    error: str | None


class SQLGenerator:
    """Orchestrates text-to-SQL generation with validation and retry."""

    MAX_RETRIES = 3

    def __init__(
        self,
        bq_client: BigQueryClient | None = None,
        gemini_client: GeminiClient | None = None,
    ):
        """Initialize the SQL generator."""
        self.bq_client = bq_client or BigQueryClient()
        self.gemini_client = gemini_client or GeminiClient()
        self.schema_builder = SchemaContextBuilder(self.bq_client)

    def generate_sql(
        self,
        question: str,
        dataset_id: str,
        table_ids: list[str] | None = None,
        conversation_history: list[dict] | None = None,
    ) -> SQLGenerationResult:
        """
        Generate SQL from a natural language question.

        Args:
            question: The user's natural language question
            dataset_id: The dataset to query
            table_ids: Optional specific tables to include in context
            conversation_history: Previous conversation turns

        Returns:
            SQLGenerationResult with generated SQL or error
        """
        # Build schema context
        schema_context = self.schema_builder.build_context(
            dataset_id,
            table_ids,
            include_row_counts=True,
        )

        # Build prompt
        prompt = PromptTemplates.build_text_to_sql_prompt(
            question,
            schema_context,
            conversation_history,
        )

        # Generate SQL with retry logic
        last_sql = None
        last_error = None

        for attempt in range(self.MAX_RETRIES):
            try:
                # Generate SQL
                if attempt == 0:
                    response = self.gemini_client.generate(
                        prompt=prompt,
                        system_instruction=PromptTemplates.SYSTEM_INSTRUCTION,
                        temperature=0.1,
                    )
                else:
                    # Retry with error context
                    retry_prompt = PromptTemplates.build_error_retry_prompt(
                        question,
                        last_sql,
                        last_error,
                        schema_context,
                    )
                    response = self.gemini_client.generate(
                        prompt=retry_prompt,
                        system_instruction=PromptTemplates.SYSTEM_INSTRUCTION,
                        temperature=0.2,  # Slightly higher for retry
                    )

                # Extract SQL from response
                sql = self.gemini_client.extract_sql(response)
                if not sql:
                    last_error = "Could not extract SQL from response"
                    continue

                last_sql = sql

                # Validate SQL (SELECT-only)
                try:
                    sql = SQLValidator.validate(sql)
                except SQLValidationError as e:
                    last_error = str(e)
                    continue

                # Dry-run validation
                is_valid, error = self.bq_client.validate_query_syntax(sql)
                if not is_valid:
                    last_error = error
                    continue

                # Success!
                return SQLGenerationResult(
                    success=True,
                    sql=sql,
                    error=None,
                    attempts=attempt + 1,
                )

            except Exception as e:
                last_error = str(e)
                continue

        # All retries failed
        return SQLGenerationResult(
            success=False,
            sql=last_sql,
            error=last_error,
            attempts=self.MAX_RETRIES,
        )

    def generate_and_execute(
        self,
        question: str,
        dataset_id: str,
        table_ids: list[str] | None = None,
        conversation_history: list[dict] | None = None,
    ) -> QueryResult:
        """
        Generate SQL and execute it.

        Args:
            question: The user's natural language question
            dataset_id: The dataset to query
            table_ids: Optional specific tables to include in context
            conversation_history: Previous conversation turns

        Returns:
            QueryResult with data or error
        """
        # Generate SQL
        gen_result = self.generate_sql(
            question,
            dataset_id,
            table_ids,
            conversation_history,
        )

        if not gen_result.success:
            return QueryResult(
                success=False,
                sql=gen_result.sql or "",
                data=None,
                row_count=0,
                error=gen_result.error,
            )

        # Execute SQL
        try:
            df = self.bq_client.execute_query(gen_result.sql, validate=True)
            return QueryResult(
                success=True,
                sql=gen_result.sql,
                data=df,
                row_count=len(df),
                error=None,
            )
        except (SQLValidationError, QueryExecutionError) as e:
            return QueryResult(
                success=False,
                sql=gen_result.sql,
                data=None,
                row_count=0,
                error=str(e),
            )

    def explain_sql(self, sql: str, question: str) -> str:
        """
        Generate an explanation of a SQL query.

        Args:
            sql: The SQL query to explain
            question: The original question

        Returns:
            Explanation text
        """
        prompt = PromptTemplates.build_explanation_prompt(sql, question)
        return self.gemini_client.generate(
            prompt=prompt,
            temperature=0.3,
        )

    def suggest_questions(
        self,
        dataset_id: str,
        table_ids: list[str] | None = None,
        num_suggestions: int = 5,
    ) -> list[str]:
        """
        Suggest questions a user could ask about the data.

        Args:
            dataset_id: The dataset to analyze
            table_ids: Optional specific tables
            num_suggestions: Number of suggestions to generate

        Returns:
            List of suggested questions
        """
        schema_context = self.schema_builder.build_context(
            dataset_id,
            table_ids,
            include_row_counts=True,
        )

        prompt = f"""## Database Schema
{schema_context}

Based on this database schema, suggest {num_suggestions} interesting questions that could be answered with SQL queries.

Format your response as a numbered list:
1. [question]
2. [question]
...

Focus on questions that would provide valuable business insights."""

        response = self.gemini_client.generate(
            prompt=prompt,
            temperature=0.7,
        )

        # Parse the response
        questions = []
        for line in response.strip().split("\n"):
            line = line.strip()
            if line and line[0].isdigit():
                # Remove number prefix
                parts = line.split(".", 1)
                if len(parts) > 1:
                    questions.append(parts[1].strip())
                else:
                    questions.append(line)

        return questions[:num_suggestions]
