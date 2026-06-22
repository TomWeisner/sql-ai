"""Session-state helpers for the Streamlit app."""

from __future__ import annotations

from datetime import datetime

import streamlit as st
from sql_ai.sql_llm import SqlLLM


def init_session_state(default_question: str, llm: SqlLLM) -> None:
    if st.session_state.get("suppress_default_question"):
        effective_default = ""
        st.session_state["suppress_default_question"] = False
    else:
        effective_default = default_question
    defaults: dict[str, object] = {
        "chat_history": [],
        "last_user_input": "",
        "default_question": effective_default,
        "has_asked_question": False,
        "error_traceback": None,
        "retry_triggered": False,
        "sql_query": None,
        "sql_prompt": None,
        "data_prompt": None,
        "format_logs": None,
        "results_df": None,
        "answer": None,
        "query_runs": [],
        "tables_loaded": False,
        "steps_taken": [],
        "keep_context": True,
        "use_supplied_sql": False,
        "dry_run": True,
        "interpret_with_llm": True,
        "show_tabs": True,
        "model_key": llm.config.bedrock_model_key,
        "selected_table_ids": [],
        "tables_selection_initialized": False,
    }
    for key, value in defaults.items():
        st.session_state.setdefault(key, value)
    if (
        effective_default
        and not st.session_state.get("has_asked_question")
        and "chat_input" not in st.session_state
    ):
        st.session_state["chat_input"] = effective_default


def get_question(input_label: str) -> str | None:
    if any(
        run.get("status") == "pending" for run in st.session_state.get("query_runs", [])
    ):
        return None
    if st.session_state.retry_triggered:
        st.session_state.retry_triggered = False
        return st.session_state.last_user_input

    user_input = st.chat_input(input_label, key="chat_input")
    if user_input:
        st.session_state.last_user_input = user_input
        st.session_state["has_asked_question"] = True
        st.session_state["steps_taken"] = []
        from sql_ai.streamlit.utils import render_sidebar_steps

        render_sidebar_steps([])
        st.session_state["pending_question_time"] = datetime.now().strftime("%H:%M:%S")
        return user_input
    return None
