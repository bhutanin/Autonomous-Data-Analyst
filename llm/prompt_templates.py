"""Prompt templates for text-to-SQL generation."""


class PromptTemplates:
    """Templates for LLM prompts."""

    SYSTEM_INSTRUCTION = """You are a BigQuery SQL expert assistant. Your role is to help users query their data by generating accurate, efficient SQL queries.

IMPORTANT RULES:
1. ONLY generate SELECT queries. Never generate INSERT, UPDATE, DELETE, DROP, CREATE, or any other data-modifying statements.
2. Always use fully qualified table names with backticks: `project.dataset.table`
3. Use BigQuery SQL dialect (not MySQL, PostgreSQL, etc.)
4. Include appropriate LIMIT clauses to prevent excessive data retrieval
5. Use column aliases for clarity when using functions or expressions
6. Handle NULL values appropriately
7. For date/time operations, use BigQuery's date functions (DATE, TIMESTAMP, EXTRACT, etc.)

OUTPUT FORMAT:
- Return ONLY the SQL query in a markdown code block
- No explanations before or after unless specifically asked
- Format the SQL for readability with proper indentation

If you cannot generate a valid SELECT query for the user's request, explain why instead of generating unsafe SQL."""

    @classmethod
    def build_text_to_sql_prompt(
        cls,
        user_question: str,
        schema_context: str,
        conversation_history: list[dict] | None = None,
    ) -> str:
        """
        Build a prompt for text-to-SQL generation.

        Args:
            user_question: The user's natural language question
            schema_context: Formatted schema information
            conversation_history: Optional previous conversation turns

        Returns:
            The formatted prompt
        """
        prompt_parts = []

        # Add schema context
        prompt_parts.append("## Database Schema\n")
        prompt_parts.append(schema_context)
        prompt_parts.append("\n")

        # Add conversation history if present
        if conversation_history:
            prompt_parts.append("## Previous Conversation\n")
            for turn in conversation_history[-5:]:  # Last 5 turns
                if turn.get("question"):
                    prompt_parts.append(f"User: {turn['question']}\n")
                if turn.get("sql"):
                    prompt_parts.append(f"SQL Generated:\n```sql\n{turn['sql']}\n```\n")
            prompt_parts.append("\n")

        # Add current question
        prompt_parts.append("## Current Question\n")
        prompt_parts.append(f"{user_question}\n\n")
        prompt_parts.append("Generate a BigQuery SQL query to answer this question.")

        return "\n".join(prompt_parts)

    @classmethod
    def build_error_retry_prompt(
        cls,
        original_question: str,
        failed_sql: str,
        error_message: str,
        schema_context: str,
    ) -> str:
        """
        Build a prompt for retrying after an error.

        Args:
            original_question: The original user question
            failed_sql: The SQL that failed
            error_message: The error message from BigQuery
            schema_context: Formatted schema information

        Returns:
            The formatted retry prompt
        """
        return f"""## Database Schema
{schema_context}

## Original Question
{original_question}

## Failed SQL Query
```sql
{failed_sql}
```

## Error Message
{error_message}

## Task
The above SQL query failed with the given error. Please fix the query and generate a corrected version.
Only return the corrected SQL query in a code block, no explanations."""

    @classmethod
    def build_explanation_prompt(cls, sql: str, question: str) -> str:
        """
        Build a prompt to explain a SQL query.

        Args:
            sql: The SQL query to explain
            question: The original question

        Returns:
            The formatted prompt
        """
        return f"""## Original Question
{question}

## SQL Query
```sql
{sql}
```

Please explain what this SQL query does in simple terms:
1. What tables and columns are being used?
2. What filtering or conditions are applied?
3. How are the results grouped or ordered?
4. What will the output look like?

Keep the explanation concise and accessible to non-technical users."""

    @classmethod
    def build_schema_summary_prompt(cls, schema_context: str) -> str:
        """
        Build a prompt to summarize a database schema.

        Args:
            schema_context: Formatted schema information

        Returns:
            The formatted prompt
        """
        return f"""## Database Schema
{schema_context}

Please provide a brief summary of this database schema:
1. What kind of data does this database contain?
2. What are the main entities/tables?
3. What kinds of questions could be answered with this data?

Keep the summary concise (3-5 sentences)."""
