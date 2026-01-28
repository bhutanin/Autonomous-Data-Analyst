"""Chat interface component."""
import streamlit as st
import pandas as pd

from core.bigquery_client import BigQueryClient
from llm.sql_generator import SQLGenerator
from ui.session_manager import SessionManager, ConversationTurn


def render_chat_interface(bq_client: BigQueryClient, dataset: str) -> None:
    """
    Render the chat interface.

    Args:
        bq_client: BigQuery client
        dataset: Selected dataset ID
    """
    # Display conversation history
    history = SessionManager.get_conversation_history()

    for turn in history:
        _render_conversation_turn(turn)

    # Chat input
    user_input = st.chat_input("Ask a question about your data...")

    if user_input:
        _handle_user_input(bq_client, dataset, user_input)


def _render_conversation_turn(turn: ConversationTurn) -> None:
    """Render a single conversation turn."""
    # User message
    with st.chat_message("user"):
        st.write(turn.question)

    # Assistant response
    with st.chat_message("assistant"):
        if turn.error:
            st.error(f"Error: {turn.error}")
            if turn.sql:
                with st.expander("Failed SQL"):
                    st.code(turn.sql, language="sql")
        else:
            # Show SQL
            with st.expander("SQL Query", expanded=False):
                st.code(turn.sql, language="sql")

            # Show results
            if turn.result is not None and not turn.result.empty:
                st.dataframe(turn.result, use_container_width=True)
                st.caption(f"{len(turn.result)} rows returned")
            elif turn.result is not None:
                st.info("Query returned no results.")

            # Show explanation if available
            if turn.explanation:
                with st.expander("Explanation"):
                    st.write(turn.explanation)


def _handle_user_input(bq_client: BigQueryClient, dataset: str, user_input: str) -> None:
    """Handle user input and generate response."""
    # Display user message immediately
    with st.chat_message("user"):
        st.write(user_input)

    # Generate and display response
    with st.chat_message("assistant"):
        _generate_response(bq_client, dataset, user_input)


def _generate_response(bq_client: BigQueryClient, dataset: str, question: str) -> None:
    """Generate SQL and execute query."""
    selected_tables = st.session_state.get("chatbot_selected_tables")
    history = SessionManager.get_history_for_prompt()

    generator = SQLGenerator(bq_client)

    # Show thinking indicator
    with st.spinner("Thinking..."):
        result = generator.generate_and_execute(
            question=question,
            dataset_id=dataset,
            table_ids=selected_tables,
            conversation_history=history,
        )

    # Create and save conversation turn
    turn = ConversationTurn(
        question=question,
        sql=result.sql if result.sql else None,
        result=result.data if result.success else None,
        error=result.error if not result.success else None,
    )
    SessionManager.add_conversation_turn(turn)

    # Display response
    if result.error:
        st.error(f"Error: {result.error}")
        if result.sql:
            with st.expander("Failed SQL"):
                st.code(result.sql, language="sql")
    else:
        # Show SQL
        with st.expander("SQL Query", expanded=False):
            st.code(result.sql, language="sql")

        # Show results
        if result.data is not None and not result.data.empty:
            st.dataframe(result.data, use_container_width=True)
            st.caption(f"{len(result.data)} rows returned")
        else:
            st.info("Query returned no results.")


def render_suggested_questions(bq_client: BigQueryClient, dataset: str) -> None:
    """Render suggested questions for the user."""
    if st.button("Suggest Questions", use_container_width=True):
        with st.spinner("Generating suggestions..."):
            try:
                generator = SQLGenerator(bq_client)
                suggestions = generator.suggest_questions(dataset)

                st.markdown("**Try asking:**")
                for suggestion in suggestions:
                    st.markdown(f"- {suggestion}")
            except Exception as e:
                st.error(f"Failed to generate suggestions: {str(e)}")
