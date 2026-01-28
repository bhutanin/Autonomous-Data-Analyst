"""UI pages module."""
from .data_profiling import render_data_profiling_page
from .relationships import render_relationships_page
from .chatbot import render_chatbot_page

__all__ = [
    "render_data_profiling_page",
    "render_relationships_page",
    "render_chatbot_page",
]
