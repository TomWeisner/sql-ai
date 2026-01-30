"""Footer action controls for the Streamlit chat UI."""

from __future__ import annotations

import streamlit as st
import streamlit.components.v1 as components
from sql_ai.streamlit.ui_templates import (
    button_class_script_html,
    tab_scroll_lock_script_html,
)


def render_footer_controls(
    *,
    execute_saved_sql,
    interpret_saved_result,
) -> None:
    if st.session_state.last_user_input:
        st.markdown("<div class='footer-controls-gap'></div>", unsafe_allow_html=True)
        col_retry, col_clear, col_tabs, col_action, col_spacer, col_question, col_top = (
            st.columns([0.4, 0.4, 0.4, 2.1, 4.5, 1.3, 0.9], gap="small")
        )
        with col_retry:
            if st.button("🔁", help="Retry last question"):
                st.session_state.retry_triggered = True
                st.rerun()
        with col_clear:
            if st.button("🗑️", help="Clear chat"):
                for key in list(st.session_state.keys()):
                    del st.session_state[key]
                st.session_state["suppress_default_question"] = True
                st.session_state["query_runs"] = []
                st.rerun()
        runs = st.session_state.get("query_runs", [])
        last_completed_idx = next(
            (
                idx
                for idx in range(len(runs) - 1, -1, -1)
                if runs[idx].get("status") == "complete"
            ),
            None,
        )
        if last_completed_idx is not None:
            with col_tabs:
                run_show_tabs = runs[last_completed_idx].get("show_tabs")
                run_tabs_override = runs[last_completed_idx].get(
                    "show_tabs_override", False
                )
                effective_show_tabs = (
                    run_show_tabs
                    if run_tabs_override
                    else st.session_state.get("show_tabs", True)
                )
                tabs_icon = "🙈" if effective_show_tabs else "🔍"
                tabs_help = (
                    "Hide tabs for the latest answer"
                    if effective_show_tabs
                    else "Show tabs for the latest answer"
                )
                if st.button(
                    tabs_icon,
                    key=f"toggle_tabs_footer_{last_completed_idx}",
                    help=tabs_help,
                ):
                    st.session_state.query_runs[last_completed_idx].update(
                        {
                            "show_tabs": not effective_show_tabs,
                            "show_tabs_override": True,
                        }
                    )
                    st.rerun()
        runs = st.session_state.get("query_runs", [])
        last_run_idx = len(runs) - 1 if runs else None
        action_label = None
        action_kind = None
        if last_run_idx is not None:
            last_run = runs[last_run_idx]
            if last_run.get("dry_run") and last_run.get("sql_query"):
                if not last_run.get("error_traceback"):
                    action_label = "▶️ Run SQL"
                    action_kind = "run_sql"
            elif last_run.get("ran_sql_only"):
                action_label = "🧠 Interpret data"
                action_kind = "interpret"
        with col_action:
            if action_label and action_kind:
                if st.button(action_label, key=f"footer_action_{action_kind}"):
                    if action_kind == "run_sql":
                        execute_saved_sql(
                            last_run_idx,
                            runs[last_run_idx],
                            st.session_state.get("interpret_with_llm", True),
                        )
                        st.rerun()
                    elif action_kind == "interpret":
                        interpret_saved_result(last_run_idx, runs[last_run_idx])
                        st.rerun()
        with col_question:
            st.markdown(
                """
                <button class="btn-scroll footer-scroll-btn" type="button"
                  data-scroll-target="current-question">
                  ⬆️&nbsp;Question
                </button>
                """,
                unsafe_allow_html=True,
            )
        with col_top:
            st.markdown(
                """
                <button class="btn-scroll footer-scroll-btn" type="button"
                  data-scroll-target="page-top">
                  ⏫&nbsp;Top
                </button>
                """,
                unsafe_allow_html=True,
            )
        components.html(button_class_script_html(), height=0)
        components.html(tab_scroll_lock_script_html(), height=0)
