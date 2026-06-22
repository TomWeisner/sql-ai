"""Top-row query option controls for the Streamlit app."""

from __future__ import annotations

from dataclasses import dataclass

import streamlit as st
import streamlit.components.v1 as components
from sql_ai.streamlit.ui_templates import (
    chat_input_reset_script_html,
    copy_button_script_html,
    toggle_class_script_html,
)


@dataclass(frozen=True)
class QueryControls:
    keep_context: bool
    use_supplied_sql: bool
    dry_run: bool
    interpret_with_llm: bool


def render_query_controls() -> QueryControls:
    col_left, col_mid, col_right, col_tabs, col_llm = st.columns([1, 1, 1, 1, 1])
    with col_left:
        st.checkbox(
            "Memory",
            key="keep_context",
            help=(
                "When on, previous Q&A remain in chat history so "
                "the model can use prior context."
            ),
        )
    with col_mid:
        st.checkbox(
            "Tabs",
            key="show_tabs",
            help="Toggle the display of additional info tabs in the answer.",
        )
    with col_right:
        st.checkbox(
            "Dry run",
            key="dry_run",
            help="Generate SQL and formatting logs but skip executing the query.",
        )
    with col_tabs:
        st.checkbox(
            "Interpret",
            key="interpret_with_llm",
            help="If enabled, the LLM will interpret query results.",
        )
    with col_llm:
        st.checkbox(
            "Use SQL",
            key="use_supplied_sql",
            help=(
                "If checked, you can paste SQL instead of generating "
                "it from natural language."
            ),
        )
    components.html(toggle_class_script_html(), height=0)
    components.html(copy_button_script_html(), height=0)
    components.html(chat_input_reset_script_html(), height=0)
    return QueryControls(
        keep_context=st.session_state.get("keep_context", True),
        use_supplied_sql=st.session_state.get("use_supplied_sql", False),
        dry_run=st.session_state.get("dry_run", True),
        interpret_with_llm=st.session_state.get("interpret_with_llm", True),
    )
