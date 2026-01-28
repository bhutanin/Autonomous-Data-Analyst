"""Vertex AI Gemini client integration."""
from dataclasses import dataclass
from typing import Any

from google import genai
from google.genai import types

from config.settings import get_settings
from core.exceptions import LLMError


@dataclass
class ChatMessage:
    """A message in the chat history."""

    role: str  # 'user' or 'model'
    content: str


class GeminiClient:
    """Client for interacting with Gemini via the google-genai SDK."""

    def __init__(self, model: str | None = None, location: str | None = None):
        """Initialize the Gemini client."""
        settings = get_settings()
        self.model = model or settings.gemini_model
        self.location = location or settings.vertex_ai_location
        self.project_id = settings.project_id
        self._client: genai.Client | None = None

    @property
    def client(self) -> genai.Client:
        """Lazy-load the Gemini client."""
        if self._client is None:
            self._client = genai.Client(
                vertexai=True,
                project=self.project_id,
                location=self.location,
            )
        return self._client

    def generate(
        self,
        prompt: str,
        system_instruction: str | None = None,
        temperature: float = 0.1,
        max_tokens: int = 2048,
    ) -> str:
        """
        Generate a response from Gemini.

        Args:
            prompt: The user prompt
            system_instruction: Optional system instruction
            temperature: Temperature for generation (lower = more deterministic)
            max_tokens: Maximum tokens to generate

        Returns:
            The generated text response
        """
        try:
            config = types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
                system_instruction=system_instruction,
            )

            response = self.client.models.generate_content(
                model=self.model,
                contents=prompt,
                config=config,
            )

            if not response.text:
                raise LLMError("Empty response from Gemini", prompt=prompt)

            return response.text.strip()

        except Exception as e:
            if isinstance(e, LLMError):
                raise
            raise LLMError(f"Gemini generation failed: {str(e)}", prompt=prompt)

    def chat(
        self,
        messages: list[ChatMessage],
        system_instruction: str | None = None,
        temperature: float = 0.1,
        max_tokens: int = 2048,
    ) -> str:
        """
        Generate a response in a multi-turn chat context.

        Args:
            messages: List of previous chat messages
            system_instruction: Optional system instruction
            temperature: Temperature for generation
            max_tokens: Maximum tokens to generate

        Returns:
            The generated text response
        """
        try:
            # Convert messages to Gemini format
            contents = []
            for msg in messages:
                contents.append(
                    types.Content(
                        role=msg.role,
                        parts=[types.Part(text=msg.content)],
                    )
                )

            config = types.GenerateContentConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
                system_instruction=system_instruction,
            )

            response = self.client.models.generate_content(
                model=self.model,
                contents=contents,
                config=config,
            )

            if not response.text:
                raise LLMError("Empty response from Gemini")

            return response.text.strip()

        except Exception as e:
            if isinstance(e, LLMError):
                raise
            raise LLMError(f"Gemini chat failed: {str(e)}")

    def extract_sql(self, response: str) -> str | None:
        """
        Extract SQL from a model response.

        Handles various formats including markdown code blocks.

        Args:
            response: The raw model response

        Returns:
            Extracted SQL or None if not found
        """
        import re

        # Try to find SQL in code blocks
        patterns = [
            r"```sql\s*(.*?)\s*```",
            r"```\s*(SELECT.*?)\s*```",
            r"```\s*(WITH.*?)\s*```",
        ]

        for pattern in patterns:
            match = re.search(pattern, response, re.DOTALL | re.IGNORECASE)
            if match:
                return match.group(1).strip()

        # If no code block, try to find raw SQL
        lines = response.strip().split("\n")
        sql_lines = []
        in_sql = False

        for line in lines:
            stripped = line.strip()
            if stripped.upper().startswith(("SELECT", "WITH")):
                in_sql = True
            if in_sql:
                sql_lines.append(line)
                if stripped.endswith(";"):
                    break

        if sql_lines:
            sql = "\n".join(sql_lines)
            # Remove trailing semicolon for BigQuery
            if sql.strip().endswith(";"):
                sql = sql.strip()[:-1]
            return sql

        return None
