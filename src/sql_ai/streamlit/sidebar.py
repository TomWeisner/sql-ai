"""Sidebar layout helpers for the Streamlit app."""

from __future__ import annotations

import streamlit as st
from sql_ai.sql_llm import SqlLLM
from sql_ai.streamlit.sidebar_models import render_models_panel
from sql_ai.streamlit.sidebar_steps import init_steps_sidebar, render_steps_taken
from sql_ai.streamlit.sidebar_tables import render_tables_panel


def render_sidebar(
    llm: SqlLLM,
    all_tables,
    format_table_description,
) -> list:
    render_models_panel(llm)
    all_tables = render_tables_panel(llm, all_tables, format_table_description)
    init_steps_sidebar()
    render_steps_taken(st.session_state.get("steps_taken", []))
    return all_tables
