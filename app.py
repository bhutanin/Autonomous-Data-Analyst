"""
Autonomous Data Analyst - Main Streamlit Application

A web application for:
1. Data Profiling - Extended statistics for BigQuery tables
2. Relationship Detection - Auto-detect table relationships
3. Natural Language Chatbot - Text-to-SQL with conversation memory
"""
import streamlit as st

from config.settings import get_settings
from core.bigquery_client import BigQueryClient
from ui.session_manager import SessionManager
from ui.pages.data_profiling import render_data_profiling_page
from ui.pages.relationships import render_relationships_page
from ui.pages.chatbot import render_chatbot_page


def main():
    """Main application entry point."""
    # Page configuration
    st.set_page_config(
        page_title="Data Analyst",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    # Initialize session state
    SessionManager.initialize()

    # Sidebar
    _render_sidebar()

    # Main content area - tabs for different features
    tab1, tab2, tab3 = st.tabs([
        "💬 Data Chat",
        "📊 Data Profiling",
        "🔗 Relationships",
    ])

    # Initialize BigQuery client
    try:
        bq_client = BigQueryClient()
    except Exception as e:
        st.error(f"Failed to initialize BigQuery client: {str(e)}")
        st.info(
            "Make sure you have set up Application Default Credentials:\n"
            "```\n"
            "gcloud auth application-default login\n"
            "```"
        )
        return

    with tab1:
        render_chatbot_page(bq_client)

    with tab2:
        render_data_profiling_page(bq_client)

    with tab3:
        render_relationships_page(bq_client)


def _render_sidebar():
    """Render the sidebar with app info and settings."""
    with st.sidebar:
        st.title("📊 Data Analyst")
        st.markdown("---")

        # Project info
        try:
            settings = get_settings()
            st.markdown(f"**Project:** `{settings.project_id}`")
        except Exception:
            st.warning("Project not configured")

        st.markdown("---")

        # Help section
        with st.expander("ℹ️ Help"):
            st.markdown("""
            **Data Chat**
            - Ask questions in plain English
            - I'll generate and run SQL queries
            - Follow-up questions remember context

            **Data Profiling**
            - Select a table to analyze
            - View statistics, null rates, distributions

            **Relationships**
            - Auto-detect table relationships
            - View as interactive graph
            """)

        # Footer
        st.markdown("---")
        st.caption("Built with Streamlit & Gemini")


if __name__ == "__main__":
    main()
