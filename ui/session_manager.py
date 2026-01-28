"""Streamlit session state management."""
from dataclasses import dataclass, field
from typing import Any

import streamlit as st
import pandas as pd


@dataclass
class ConversationTurn:
    """A single turn in the conversation."""

    question: str
    sql: str | None
    result: pd.DataFrame | None
    error: str | None
    explanation: str | None = None


class SessionManager:
    """Manages Streamlit session state for the application."""

    # Session state keys
    SELECTED_DATASET = "selected_dataset"
    SELECTED_TABLE = "selected_table"
    CONVERSATION_HISTORY = "conversation_history"
    CACHED_SCHEMAS = "cached_schemas"
    CACHED_PROFILES = "cached_profiles"
    CACHED_RELATIONSHIPS = "cached_relationships"

    @classmethod
    def initialize(cls) -> None:
        """Initialize session state with default values."""
        if cls.SELECTED_DATASET not in st.session_state:
            st.session_state[cls.SELECTED_DATASET] = None

        if cls.SELECTED_TABLE not in st.session_state:
            st.session_state[cls.SELECTED_TABLE] = None

        if cls.CONVERSATION_HISTORY not in st.session_state:
            st.session_state[cls.CONVERSATION_HISTORY] = []

        if cls.CACHED_SCHEMAS not in st.session_state:
            st.session_state[cls.CACHED_SCHEMAS] = {}

        if cls.CACHED_PROFILES not in st.session_state:
            st.session_state[cls.CACHED_PROFILES] = {}

        if cls.CACHED_RELATIONSHIPS not in st.session_state:
            st.session_state[cls.CACHED_RELATIONSHIPS] = {}

    # Dataset/Table Selection
    @classmethod
    def get_selected_dataset(cls) -> str | None:
        """Get the currently selected dataset."""
        return st.session_state.get(cls.SELECTED_DATASET)

    @classmethod
    def set_selected_dataset(cls, dataset: str | None) -> None:
        """Set the selected dataset."""
        st.session_state[cls.SELECTED_DATASET] = dataset
        # Clear table selection when dataset changes
        st.session_state[cls.SELECTED_TABLE] = None

    @classmethod
    def get_selected_table(cls) -> str | None:
        """Get the currently selected table."""
        return st.session_state.get(cls.SELECTED_TABLE)

    @classmethod
    def set_selected_table(cls, table: str | None) -> None:
        """Set the selected table."""
        st.session_state[cls.SELECTED_TABLE] = table

    # Conversation History
    @classmethod
    def get_conversation_history(cls) -> list[ConversationTurn]:
        """Get the conversation history."""
        return st.session_state.get(cls.CONVERSATION_HISTORY, [])

    @classmethod
    def add_conversation_turn(cls, turn: ConversationTurn) -> None:
        """Add a turn to the conversation history."""
        history = cls.get_conversation_history()
        history.append(turn)
        st.session_state[cls.CONVERSATION_HISTORY] = history

    @classmethod
    def clear_conversation_history(cls) -> None:
        """Clear the conversation history."""
        st.session_state[cls.CONVERSATION_HISTORY] = []

    @classmethod
    def get_history_for_prompt(cls) -> list[dict]:
        """Get conversation history formatted for LLM prompt."""
        history = cls.get_conversation_history()
        return [
            {
                "question": turn.question,
                "sql": turn.sql,
            }
            for turn in history
            if turn.sql  # Only include successful queries
        ]

    # Schema Caching
    @classmethod
    def get_cached_schema(cls, dataset: str) -> dict | None:
        """Get cached schema for a dataset."""
        return st.session_state.get(cls.CACHED_SCHEMAS, {}).get(dataset)

    @classmethod
    def set_cached_schema(cls, dataset: str, schema: dict) -> None:
        """Cache schema for a dataset."""
        if cls.CACHED_SCHEMAS not in st.session_state:
            st.session_state[cls.CACHED_SCHEMAS] = {}
        st.session_state[cls.CACHED_SCHEMAS][dataset] = schema

    # Profile Caching
    @classmethod
    def get_cached_profile(cls, table_key: str) -> Any | None:
        """Get cached profile for a table."""
        return st.session_state.get(cls.CACHED_PROFILES, {}).get(table_key)

    @classmethod
    def set_cached_profile(cls, table_key: str, profile: Any) -> None:
        """Cache profile for a table."""
        if cls.CACHED_PROFILES not in st.session_state:
            st.session_state[cls.CACHED_PROFILES] = {}
        st.session_state[cls.CACHED_PROFILES][table_key] = profile

    # Relationship Caching
    @classmethod
    def get_cached_relationships(cls, dataset: str) -> list | None:
        """Get cached relationships for a dataset."""
        return st.session_state.get(cls.CACHED_RELATIONSHIPS, {}).get(dataset)

    @classmethod
    def set_cached_relationships(cls, dataset: str, relationships: list) -> None:
        """Cache relationships for a dataset."""
        if cls.CACHED_RELATIONSHIPS not in st.session_state:
            st.session_state[cls.CACHED_RELATIONSHIPS] = {}
        st.session_state[cls.CACHED_RELATIONSHIPS][dataset] = relationships

    # Utility Methods
    @classmethod
    def clear_all_caches(cls) -> None:
        """Clear all cached data."""
        st.session_state[cls.CACHED_SCHEMAS] = {}
        st.session_state[cls.CACHED_PROFILES] = {}
        st.session_state[cls.CACHED_RELATIONSHIPS] = {}

    @classmethod
    def reset_session(cls) -> None:
        """Reset the entire session state."""
        for key in list(st.session_state.keys()):
            del st.session_state[key]
        cls.initialize()
