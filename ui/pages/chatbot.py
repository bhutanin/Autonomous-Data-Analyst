"""Natural language chatbot page."""
import streamlit as st
import pandas as pd

from core.bigquery_client import BigQueryClient
from core.sql_validator import SQLValidationError
from llm.sql_generator import SQLGenerator
from ui.session_manager import SessionManager, ConversationTurn
from ui.components.table_selector import render_dataset_selector
from ui.components.chat_interface import render_chat_interface


def render_chatbot_page(bq_client: BigQueryClient) -> None:
    """Render the natural language chatbot page."""
    st.header("Data Chat")
    st.markdown(
        "Ask questions about your data in plain English. "
        "I'll generate and run SQL queries to find the answers."
    )

    # Dataset selection in sidebar
    with st.sidebar:
        st.subheader("Query Settings")
        dataset = render_dataset_selector(bq_client, key_prefix="chatbot")

        if dataset:
            # Show available tables
            tables = bq_client.list_tables(dataset)
            with st.expander("Available Tables", expanded=False):
                for table in tables:
                    row_info = f" ({table.num_rows:,} rows)" if table.num_rows else ""
                    st.markdown(f"- `{table.table}`{row_info}")

            # Option to select specific tables for context
            table_names = [t.table for t in tables]
            selected_tables = st.multiselect(
                "Focus on tables (optional)",
                options=table_names,
                help="Limit context to specific tables for better results",
            )

            st.divider()

            if st.button("Clear Chat History", use_container_width=True):
                SessionManager.clear_conversation_history()
                st.rerun()

    if not dataset:
        st.info("Select a dataset from the sidebar to start chatting.")
        return

    # Store selected tables in session for the chat handler
    st.session_state["chatbot_selected_tables"] = selected_tables if selected_tables else None

    # Render chat interface
    render_chat_interface(bq_client, dataset)


def handle_user_message(
    bq_client: BigQueryClient,
    dataset: str,
    user_message: str,
) -> None:
    """Process a user message and generate response."""
    selected_tables = st.session_state.get("chatbot_selected_tables")
    history = SessionManager.get_history_for_prompt()

    # Create SQL generator
    generator = SQLGenerator(bq_client)

    with st.spinner("Generating SQL..."):
        result = generator.generate_and_execute(
            question=user_message,
            dataset_id=dataset,
            table_ids=selected_tables,
            conversation_history=history,
        )

    # Create conversation turn
    turn = ConversationTurn(
        question=user_message,
        sql=result.sql if result.sql else None,
        result=result.data if result.success else None,
        error=result.error if not result.success else None,
    )

    SessionManager.add_conversation_turn(turn)
