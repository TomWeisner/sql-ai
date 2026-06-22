"""Conversation rendering helpers for the Streamlit chat UI."""

from __future__ import annotations

from datetime import datetime
from typing import Callable, Optional

import pandas as pd

import streamlit as st
from sql_ai.streamlit.ui_templates import (
    assistant_answer_html,
    message_time_html,
    user_message_html,
)
from sql_ai.tracking.decorator import track_step_and_log_cm


def render_user_message(question: str, timestamp: str | None, attrs: str = "") -> None:
    st.markdown(user_message_html(question, attrs=attrs), unsafe_allow_html=True)
    if timestamp:
        st.markdown(
            message_time_html(timestamp, "right"),
            unsafe_allow_html=True,
        )


def render_assistant_message(
    message: str, timestamp: str | None, pulse: bool = False
) -> None:
    st.markdown(assistant_answer_html(message, pulse=pulse), unsafe_allow_html=True)
    if timestamp:
        st.markdown(
            message_time_html(timestamp, "left"),
            unsafe_allow_html=True,
        )


def render_conversation(
    *,
    llm,
    handle_question_actual: Callable[[str, bool, bool, bool], Optional[dict]],
    execute_saved_sql: Callable[..., None],
    interpret_saved_result: Callable[[int, dict], None],
) -> None:
    runs = st.session_state.get("query_runs", [])
    if not runs:
        return

    for idx, run in enumerate(runs):
        is_last_run = idx == len(runs) - 1
        anchor_attrs = "data-question-anchor='true'"
        if is_last_run:
            anchor_attrs += " data-current-question='true'"
        anchor_id = "current-question" if is_last_run else f"question-{idx}"
        attrs = f"id='{anchor_id}' {anchor_attrs}"
        asked_at = run.get("asked_at")
        render_user_message(run.get("question", "(unknown)"), asked_at, attrs)
        if run.get("status") == "pending":
            with st.container():
                previous_typewriter = st.session_state.get(
                    "suppress_sidebar_typewriter", True
                )
                st.session_state["suppress_sidebar_typewriter"] = False
                try:
                    model_name = llm.config.bedrock_model.name
                    with st.spinner(f"_Generating answer... ({model_name})_"):
                        with track_step_and_log_cm("Processing user input"):
                            result = handle_question_actual(
                                run.get("question", ""),
                                run.get("use_supplied_sql", False),
                                run.get("dry_run", False),
                                run.get("interpret_with_llm", True),
                            )
                finally:
                    st.session_state["suppress_sidebar_typewriter"] = previous_typewriter
            if result:
                status = "error" if result.get("error_message") else "complete"
                st.session_state.query_runs[idx] = {
                    **run,
                    **result,
                    "status": status,
                }
            else:
                st.session_state.query_runs[idx] = {
                    **run,
                    "status": "error",
                    "error_message": "An error occurred.",
                }
            st.rerun()
            return

        has_answer = bool(run.get("answer"))

        run_show_tabs = run.get("show_tabs")
        run_tabs_override = run.get("show_tabs_override", False)
        if is_last_run and run_tabs_override:
            effective_show_tabs = bool(run_show_tabs)
        else:
            effective_show_tabs = st.session_state.get("show_tabs", True)
        if effective_show_tabs:
            tabs: list[tuple[str, Callable[[], None]]] = []

            def _render_code_block(content: str, language: str) -> None:
                st.code(content, language)

            def _render_sql_query() -> None:
                _render_code_block(run["sql_query"], "sql")

            def _render_sql_prompt() -> None:
                _render_code_block(run["sql_prompt"], "json")

            def _render_format_logs() -> None:
                _render_code_block(run["format_logs"], "sql")

            if run.get("sql_query"):
                tabs.append(("📄 SQL", _render_sql_query))
            if run.get("sql_prompt"):
                tabs.append(("🛠 SQL Prompt", _render_sql_prompt))
            if run.get("format_logs"):
                tabs.append(("🎨 SQL Formatting", _render_format_logs))
            df = run.get("results_df")
            if df is not None:

                def _render_data_editor(dataframe: pd.DataFrame) -> None:
                    st.data_editor(
                        dataframe,
                        use_container_width=True,
                        num_rows="dynamic",
                        key=f"data_editor_{idx}",
                    )

                def _render_data_tab() -> None:
                    _render_data_editor(df)

                tabs.append(("🧮 Data", _render_data_tab))
                csv = df.to_csv(index=False).encode("utf-8")
                now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                file_name = f"results_{now_str}.csv"
                download_label = "⬇️ Download data"
                download_key = f"dl_{idx}"

                def _render_download_tab() -> None:
                    st.download_button(
                        "⬇️ Download output data.",
                        csv,
                        file_name,
                        "text/csv",
                        key=download_key,
                    )

                tabs.append((download_label, _render_download_tab))
            if run.get("data_prompt"):

                def _render_data_prompt() -> None:
                    _render_code_block(run["data_prompt"], "json")

                tabs.append(("🧾 Output prompt", _render_data_prompt))
            if tabs:
                labels, render_fns = zip(*tabs)
                for tab, render in zip(st.tabs(labels), render_fns):
                    with tab:
                        render()

        format_error = run.get("error_traceback")
        if format_error and run.get("status") != "error":
            summary = format_error.strip().splitlines()[-1] if format_error else ""
            headline = (
                f"SQL formatting error: {summary}" if summary else "SQL formatting error."
            )
            st.error(headline)
            with st.expander("Show full formatting error details"):
                st.markdown(
                    f"<pre style='color:red'>{format_error}</pre>",
                    unsafe_allow_html=True,
                )

        answered_at = run.get("answered_at")
        if not answered_at and (
            has_answer
            or run.get("dry_run")
            or run.get("status") == "error"
            or run.get("ran_sql_only")
        ):
            answered_at = datetime.now().strftime("%H:%M:%S")
            st.session_state.query_runs[idx]["answered_at"] = answered_at
        if has_answer:
            render_assistant_message(
                run["answer"],
                answered_at,
                pulse=run.get("answer_source") == "interpret",
            )
        elif run.get("status") == "error":
            error_message = run.get("error_message") or "An error occurred."
            st.error(error_message)
            exception_traceback = run.get("exception_traceback")
            if exception_traceback:
                with st.expander("Show full error details"):
                    st.markdown(
                        f"<pre style='color:red'>{exception_traceback}</pre>",
                        unsafe_allow_html=True,
                    )
            if answered_at:
                st.markdown(
                    message_time_html(answered_at, "left"),
                    unsafe_allow_html=True,
                )
        elif run.get("ran_sql_only"):
            st.markdown(
                "<div class='status-banner status-warn'>"
                "SQL executed but data not interpreted."
                "</div>",
                unsafe_allow_html=True,
            )
            if answered_at:
                st.markdown(
                    message_time_html(answered_at, "left"),
                    unsafe_allow_html=True,
                )
        elif run.get("dry_run"):
            st.markdown(
                "<div class='status-banner status-warn'>"
                "Dry run: SQL generated, not executed."
                "</div>",
                unsafe_allow_html=True,
            )
            st.markdown("<div class='status-after-gap'></div>", unsafe_allow_html=True)
            if run.get("sql_query"):
                if run.get("error_traceback"):
                    st.warning("SQL formatting failed; fix the query before running it.")
            if answered_at:
                st.markdown(
                    message_time_html(answered_at, "left"),
                    unsafe_allow_html=True,
                )
