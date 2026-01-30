"""
Streamlit app for running a chatbot with Bedrock.

This app uses our SQL LLM class to generate SQL queries.

Run the app with the below from the project root:
streamlit run src/sql_ai/streamlit/app.py
"""

import html
import logging
import os
from dataclasses import replace
from datetime import datetime

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from sql_ai.app_objects.cem_timetable import CEMLLM
from sql_ai.app_objects.pixar_films import PixarLLM
from sql_ai.bedrock.models import MODEL_REGISTRY
from sql_ai.sql_llm import SqlLLM
from sql_ai.streamlit.css_utils import (
    inject_app_styles,
    set_sidebar_width_and_center_content,
    set_title_top_padding,
)
from sql_ai.streamlit.ui_templates import (
    button_class_script_html,
    chat_input_reset_script_html,
    copy_button_script_html,
    message_time_html,
    table_desc_html,
    table_meta_html,
    toggle_class_script_html,
    user_message_html,
)
from sql_ai.streamlit.utils import (
    display_enhanced_traceback,
    neat_prompt,
    render_sidebar_steps,
    set_sidebar_steps_placeholder,
)
from sql_ai.tracking.decorator import track_step_and_log_cm

# Suppress Streamlit-specific warnings/logs
logging.getLogger("streamlit").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.scriptruncontext").setLevel(logging.ERROR)


class ChatbotApp:
    QUESTION_INPUT_LABEL = "Ask a question about the data"

    def __init__(self, athena_llm: SqlLLM, title: str, default_question: str = ""):
        self.llm = athena_llm
        saved_tables = getattr(self.llm, "all_tables", None) or getattr(
            self.llm.backend, "all_tables", None
        )
        if saved_tables:
            self.all_tables = list(saved_tables)
        else:
            base_tables = self.llm.backend.tables or self.llm.tables
            self.all_tables = list(base_tables)
            self.llm.all_tables = list(self.all_tables)
            self.llm.backend.all_tables = list(self.all_tables)
        self.title = title
        self.default_question = default_question
        self._init_session_state()

    def _init_session_state(self):
        if st.session_state.get("suppress_default_question"):
            default_question = ""
            st.session_state["suppress_default_question"] = False  # Reset it
        else:
            default_question = self.default_question
        defaults = {
            "chat_history": [],
            "last_user_input": "",
            "default_question": default_question,
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
            "model_key": self.llm.config.bedrock_model_key,
            "selected_table_ids": [],
            "tables_selection_initialized": False,
        }
        for k, v in defaults.items():
            st.session_state.setdefault(k, v)

    def run(self):
        st.session_state.setdefault("suppress_sidebar_typewriter", True)
        set_sidebar_width_and_center_content(sidebar_width=450, max_content_width=1100)
        set_title_top_padding(rem=0)
        inject_app_styles(chat_width=640)
        self._render_models_panel()
        self._render_tables_panel()
        st.sidebar.title("🧭 Steps taken")
        set_sidebar_steps_placeholder(st.sidebar.empty())
        self._render_steps_taken()
        st.title(f"🚂 LNER LLMs - {self.title}")
        self._render_query_options()

        keep_context = st.session_state.get("keep_context", True)
        use_supplied_sql = st.session_state.get("use_supplied_sql", False)
        dry_run = st.session_state.get("dry_run", True)
        interpret_with_llm = st.session_state.get("interpret_with_llm", True)

        question = self._get_question()

        if question:
            if not self.llm.tables:
                st.error("Please select at least one table to query.")
                return
            self._handle_question(
                question, keep_context, use_supplied_sql, dry_run, interpret_with_llm
            )

        self._render_conversation()

        self._render_footer_controls()

    def _get_question(self):
        if any(
            run.get("status") == "pending"
            for run in st.session_state.get("query_runs", [])
        ):
            return None
        if st.session_state.retry_triggered:
            st.session_state.retry_triggered = False
            return st.session_state.last_user_input

        user_input = st.chat_input(self.QUESTION_INPUT_LABEL)
        if user_input:
            st.session_state.last_user_input = user_input
            st.session_state["steps_taken"] = []
            render_sidebar_steps([])
            st.session_state["pending_question_time"] = datetime.now().strftime(
                "%H:%M:%S"
            )
            return user_input
        return None

    def _handle_question_actual(
        self, question, use_supplied_sql, dry_run=False, interpret_with_llm=True
    ):
        start_time = datetime.now()
        try:

            if use_supplied_sql:
                with track_step_and_log_cm("📥 Using user-supplied SQL..."):
                    sql_result = self.llm.get_sql(
                        question, use_supplied_sql=use_supplied_sql
                    )

            else:
                if st.session_state.get("keep_context", True):
                    self.llm.sql_prompt.extra_context = (
                        self._build_previous_conversation_context(question)
                    )
                else:
                    self.llm.sql_prompt.extra_context = ""
                with track_step_and_log_cm("🧠 Converting natural language to SQL..."):
                    sql_result = self.llm.get_sql(
                        question, use_supplied_sql=use_supplied_sql
                    )
                    st.session_state.update(
                        {
                            "sql_prompt": neat_prompt(sql_result.prompt_body),
                        }
                    )

            normalized_sql = self._normalize_sql(sql_result.sql)
            st.session_state.update(
                {
                    "sql_query": normalized_sql,
                    "format_logs": "\n".join(sql_result.format_logs),
                    "error_traceback": sql_result.error_traceback,
                }
            )

            if sql_result.error_traceback:
                st.error(sql_result.error_traceback)

            if dry_run:
                st.session_state.results_df = None
                st.session_state.answer = None
                return {
                    "question": question,
                    "sql_query": st.session_state.sql_query,
                    "sql_prompt": st.session_state.sql_prompt,
                    "format_logs": st.session_state.format_logs,
                    "error_traceback": st.session_state.error_traceback,
                    "results_df": None,
                    "data_prompt": None,
                    "answer": None,
                    "dry_run": True,
                    "duration_s": (datetime.now() - start_time).total_seconds(),
                }

            with track_step_and_log_cm(
                f"⚙️ Running SQL query on {self.llm.backend.name}..."
            ):
                df = self.llm.run_query(normalized_sql)
                st.session_state.results_df = df

            if interpret_with_llm:
                with track_step_and_log_cm("⏳ Interpreting data..."):
                    answer, data_prompt = self.llm.question_about_data(question, df)
                    st.session_state.update(
                        {
                            "answer": answer,
                            "data_prompt": neat_prompt(data_prompt),
                        }
                    )
                    return {
                        "question": question,
                        "sql_query": st.session_state.sql_query,
                        "sql_prompt": st.session_state.sql_prompt,
                        "format_logs": st.session_state.format_logs,
                        "error_traceback": st.session_state.error_traceback,
                        "results_df": df.copy() if isinstance(df, pd.DataFrame) else df,
                        "data_prompt": st.session_state.data_prompt,
                        "answer": answer,
                        "dry_run": False,
                        "ran_sql_only": False,
                        "duration_s": (datetime.now() - start_time).total_seconds(),
                    }
            return {
                "question": question,
                "sql_query": st.session_state.sql_query,
                "sql_prompt": st.session_state.sql_prompt,
                "format_logs": st.session_state.format_logs,
                "error_traceback": st.session_state.error_traceback,
                "results_df": df.copy() if isinstance(df, pd.DataFrame) else df,
                "data_prompt": None,
                "answer": None,
                "dry_run": False,
                "ran_sql_only": True,
                "duration_s": (datetime.now() - start_time).total_seconds(),
            }
        except Exception as e:
            error_details = display_enhanced_traceback(e)
            duration_s = (datetime.now() - start_time).total_seconds()
            return {
                "question": question,
                "sql_query": st.session_state.get("sql_query"),
                "sql_prompt": st.session_state.get("sql_prompt"),
                "format_logs": st.session_state.get("format_logs"),
                "results_df": st.session_state.get("results_df"),
                "data_prompt": st.session_state.get("data_prompt"),
                "answer": None,
                "dry_run": dry_run,
                "duration_s": duration_s,
                "error_message": (
                    error_details.get("message")
                    if isinstance(error_details, dict)
                    else "An error occurred."
                ),
                "exception_traceback": (
                    error_details.get("traceback")
                    if isinstance(error_details, dict)
                    else None
                ),
                "error_exception": (
                    error_details.get("exception_only")
                    if isinstance(error_details, dict)
                    else None
                ),
            }

    def _execute_saved_sql(
        self, idx: int, run: dict, generate_answer: bool = False
    ) -> None:
        sql_query = run.get("sql_query")
        if not sql_query:
            return
        question = run.get("question", "")
        start_time = datetime.now()
        try:
            with track_step_and_log_cm(
                f"⚙️ Running SQL query on {self.llm.backend.name}..."
            ):
                df = self.llm.run_query(sql_query)
                st.session_state.results_df = df

            update_payload = {
                **run,
                "results_df": df.copy() if isinstance(df, pd.DataFrame) else df,
                "data_prompt": None,
                "answer": None,
                "ran_sql_only": True,
                "dry_run": False,
                "status": "complete",
                "duration_s": (datetime.now() - start_time).total_seconds(),
                "answered_at": datetime.now().strftime("%H:%M:%S"),
            }
            if generate_answer:
                with track_step_and_log_cm("⏳ Generating answer..."):
                    answer, data_prompt = self.llm.question_about_data(question, df)
                    st.session_state.update(
                        {
                            "answer": answer,
                            "data_prompt": neat_prompt(data_prompt),
                        }
                    )
                update_payload.update(
                    {
                        "answer": answer,
                        "data_prompt": st.session_state.data_prompt,
                        "ran_sql_only": False,
                    }
                )

            st.session_state.query_runs[idx] = update_payload
        except Exception as e:
            error_details = display_enhanced_traceback(e)
            st.session_state.query_runs[idx] = {
                **run,
                "status": "error",
                "error_message": (
                    error_details.get("message")
                    if isinstance(error_details, dict)
                    else "An error occurred."
                ),
                "exception_traceback": (
                    error_details.get("traceback")
                    if isinstance(error_details, dict)
                    else None
                ),
                "error_exception": (
                    error_details.get("exception_only")
                    if isinstance(error_details, dict)
                    else None
                ),
                "duration_s": (datetime.now() - start_time).total_seconds(),
                "answered_at": datetime.now().strftime("%H:%M:%S"),
            }

    def _interpret_saved_result(self, idx: int, run: dict) -> None:
        df = run.get("results_df")
        if df is None:
            df = st.session_state.get("results_df")
        if df is None:
            return
        question = run.get("question", "")
        start_time = datetime.now()
        try:
            with track_step_and_log_cm("⏳ Generating answer..."):
                answer, data_prompt = self.llm.question_about_data(question, df)
                st.session_state.update(
                    {
                        "answer": answer,
                        "data_prompt": neat_prompt(data_prompt),
                    }
                )
            st.session_state.query_runs[idx] = {
                **run,
                "answer": answer,
                "data_prompt": st.session_state.data_prompt,
                "ran_sql_only": False,
                "status": "complete",
                "duration_s": (datetime.now() - start_time).total_seconds(),
                "answered_at": datetime.now().strftime("%H:%M:%S"),
            }
        except Exception as e:
            error_details = display_enhanced_traceback(e)
            st.session_state.query_runs[idx] = {
                **run,
                "status": "error",
                "error_message": (
                    error_details.get("message")
                    if isinstance(error_details, dict)
                    else "An error occurred."
                ),
                "exception_traceback": (
                    error_details.get("traceback")
                    if isinstance(error_details, dict)
                    else None
                ),
                "error_exception": (
                    error_details.get("exception_only")
                    if isinstance(error_details, dict)
                    else None
                ),
                "duration_s": (datetime.now() - start_time).total_seconds(),
                "answered_at": datetime.now().strftime("%H:%M:%S"),
            }

    def _clear_previous_variables(self):
        for k in [
            "answer",
            "results_df",
            "format_logs",
            "data_prompt",
            "sql_prompt",
            "sql_query",
        ]:
            st.session_state[k] = None

    def _handle_question(
        self,
        question,
        keep_context,
        use_supplied_sql,
        dry_run=False,
        interpret_with_llm=True,
    ):
        self._clear_previous_variables()
        if not keep_context:
            st.session_state.query_runs = []
        st.session_state["steps_taken"] = []
        asked_at = st.session_state.pop("pending_question_time", None)
        if not asked_at:
            asked_at = datetime.now().strftime("%H:%M:%S")
        st.session_state.query_runs.append(
            {
                "question": question,
                "status": "pending",
                "use_supplied_sql": use_supplied_sql,
                "dry_run": dry_run,
                "interpret_with_llm": interpret_with_llm,
                "asked_at": asked_at,
                "show_tabs": None,
                "show_tabs_override": False,
            }
        )

    def _render_conversation(self):
        runs = st.session_state.get("query_runs", [])
        if not runs:
            return

        for idx, run in enumerate(runs):
            asked_at = run.get("asked_at")
            self._render_user_message(run.get("question", "(unknown)"), asked_at)
            if run.get("status") == "pending":
                with st.container():
                    previous_typewriter = st.session_state.get(
                        "suppress_sidebar_typewriter", True
                    )
                    st.session_state["suppress_sidebar_typewriter"] = False
                    try:
                        model_name = self.llm.config.bedrock_model.name
                        with st.spinner(f"_Generating answer... ({model_name})_"):
                            with track_step_and_log_cm("Processing user input"):
                                result = self._handle_question_actual(
                                    run.get("question", ""),
                                    run.get("use_supplied_sql", False),
                                    run.get("dry_run", False),
                                    run.get("interpret_with_llm", True),
                                )
                    finally:
                        st.session_state["suppress_sidebar_typewriter"] = (
                            previous_typewriter
                        )
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

            if run.get("answer"):
                has_answer = True
            else:
                has_answer = False

            run_show_tabs = run.get("show_tabs")
            run_tabs_override = run.get("show_tabs_override", False)
            is_last_run = idx == len(runs) - 1
            if is_last_run and run_tabs_override:
                effective_show_tabs = bool(run_show_tabs)
            else:
                effective_show_tabs = st.session_state.get("show_tabs", True)
            if effective_show_tabs:
                tabs = []
                if run.get("sql_query"):
                    tabs.append(
                        ("📄 SQL", lambda sql=run["sql_query"]: st.code(sql, "sql"))
                    )
                if run.get("sql_prompt"):
                    tabs.append(
                        (
                            "🛠 SQL Prompt",
                            lambda prompt=run["sql_prompt"]: st.code(prompt, "json"),
                        )
                    )
                if run.get("format_logs"):
                    tabs.append(
                        (
                            "🎨 SQL Formatting",
                            lambda logs=run["format_logs"]: st.code(logs, "sql"),
                        )
                    )
                df = run.get("results_df")
                if df is not None:
                    tabs.append(
                        (
                            "🧮 Data",
                            lambda df=df: st.data_editor(
                                df,
                                use_container_width=True,
                                num_rows="dynamic",
                                key=f"data_editor_{idx}",
                            ),
                        )
                    )
                    csv = df.to_csv(index=False).encode("utf-8")
                    now_str = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
                    file_name = f"results_{now_str}.csv"
                    download_label = "⬇️ Download data"
                    download_key = f"dl_{idx}"

                    def _render_download_button(
                        csv_data: bytes, output_name: str, key: str
                    ) -> None:
                        st.download_button(
                            "⬇️ Download output data.",
                            csv_data,
                            output_name,
                            "text/csv",
                            key=key,
                        )

                    def _render_download_tab() -> None:
                        _render_download_button(csv, file_name, download_key)

                    tabs.append(
                        (
                            download_label,
                            _render_download_tab,
                        )
                    )
                if run.get("data_prompt"):
                    tabs.append(
                        (
                            "🧾 Output prompt",
                            lambda prompt=run["data_prompt"]: st.code(prompt, "json"),
                        )
                    )
                if tabs:
                    labels, render_fns = zip(*tabs)
                    for tab, render in zip(st.tabs(labels), render_fns):
                        with tab:
                            render()

            format_error = run.get("error_traceback")
            if format_error and run.get("status") != "error":
                summary = format_error.strip().splitlines()[-1] if format_error else ""
                headline = (
                    f"SQL formatting error: {summary}"
                    if summary
                    else "SQL formatting error."
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
                self._render_assistant_message(run["answer"], answered_at)
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
                st.markdown("*SQL executed. No LLM answer generated.*")
                if st.button(
                    "🧠 Interpret SQL result",
                    key=f"interpret_sql_{idx}",
                    help="Generate an answer from the existing query results.",
                ):
                    with st.spinner("Interpreting SQL result..."):
                        self._interpret_saved_result(idx, run)
                    st.rerun()
                if answered_at:
                    st.markdown(
                        message_time_html(answered_at, "left"),
                        unsafe_allow_html=True,
                    )
            elif run.get("dry_run"):
                st.markdown("*Dry run: SQL generated, not executed.*")
                if run.get("sql_query"):
                    if run.get("error_traceback"):
                        st.warning(
                            "SQL formatting failed; fix the query before running it."
                        )
                    else:
                        if st.button(
                            "▶️ Run SQL",
                            key=f"run_sql_{idx}",
                            help="Execute the generated SQL and answer the question.",
                        ):
                            with st.spinner("Running SQL query..."):
                                self._execute_saved_sql(
                                    idx,
                                    run,
                                    generate_answer=st.session_state.get(
                                        "interpret_with_llm", True
                                    ),
                                )
                            st.rerun()
                if answered_at:
                    st.markdown(
                        message_time_html(answered_at, "left"),
                        unsafe_allow_html=True,
                    )

    def _render_user_message(self, question: str, timestamp: str | None):
        st.markdown(user_message_html(question), unsafe_allow_html=True)
        if timestamp:
            st.markdown(
                message_time_html(timestamp, "right"),
                unsafe_allow_html=True,
            )

    def _render_assistant_message(self, message: str, timestamp: str | None):
        st.markdown(message)
        if timestamp:
            st.markdown(
                message_time_html(timestamp, "left"),
                unsafe_allow_html=True,
            )

    def _normalize_sql(self, sql: str) -> str:
        cleaned = sql.strip()
        if cleaned.startswith("```"):
            cleaned = cleaned.strip("`").strip()
            if cleaned.lower().startswith("sql"):
                cleaned = cleaned[3:].lstrip()
        return self.llm._clean_query_prefix(cleaned)

    def _render_footer_controls(self):
        if st.session_state.last_user_input:
            col_retry, col_clear, col_tabs, _ = st.columns(
                [0.4, 0.4, 0.4, 8.8], gap="small"
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
                        st.session_state.query_runs[last_completed_idx][
                            "show_tabs"
                        ] = not effective_show_tabs
                        st.session_state.query_runs[last_completed_idx][
                            "show_tabs_override"
                        ] = True
                        st.rerun()
            components.html(button_class_script_html(), height=0)

    def _render_query_options(self):
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

    def _apply_model_selection(self, model_key: str) -> None:
        if model_key not in MODEL_REGISTRY:
            return
        base_model = MODEL_REGISTRY[model_key]
        inference_profile = getattr(self.llm.config, "bedrock_inference_profile_id", "")
        if inference_profile:
            model = replace(base_model, invoke_id=inference_profile)
        else:
            model = base_model
        self.llm.config.bedrock_model_key = model_key
        self.llm.config.bedrock_model = model
        self.llm.sql_prompt.model = model

    def _render_models_panel(self):
        with st.sidebar.expander("🧠 Available models", expanded=False):
            model_keys = list(MODEL_REGISTRY.keys())
            if not model_keys:
                st.markdown("_No models available_")
                return
            current_key = st.session_state.get(
                "model_key", self.llm.config.bedrock_model_key
            )
            if current_key not in MODEL_REGISTRY:
                current_key = model_keys[0]
                st.session_state["model_key"] = current_key
            selected_key = st.radio(
                "Model",
                model_keys,
                key="model_key",
                format_func=lambda k: f"{MODEL_REGISTRY[k].name}",
                label_visibility="collapsed",
            )
            if selected_key != self.llm.config.bedrock_model_key:
                self._apply_model_selection(selected_key)

    def _render_tables_panel(self):
        with st.sidebar.expander("📚 Available tables", expanded=False):
            if not self.all_tables:
                st.markdown("_No tables available_")
                return

            if not st.session_state.get("tables_loaded"):
                with st.spinner("Loading table schemas..."):
                    try:
                        previous_typewriter = st.session_state.get(
                            "suppress_sidebar_typewriter", True
                        )
                        st.session_state["suppress_sidebar_typewriter"] = True
                        original_tables = self.llm.backend.tables
                        self.llm.backend.tables = self.all_tables
                        self.llm.backend.populate_schemas()
                        st.session_state.tables_loaded = True
                    except Exception as e:
                        st.warning(f"Could not load table schemas: {e}")
                    finally:
                        self.llm.backend.tables = original_tables
                        st.session_state["suppress_sidebar_typewriter"] = (
                            previous_typewriter
                        )

            unique_tables = []
            seen_ids = set()
            for table in self.all_tables:
                table_id = self._table_id(table)
                if table_id in seen_ids:
                    continue
                seen_ids.add(table_id)
                unique_tables.append(table)
            self.all_tables = unique_tables
            table_ids = [self._table_id(t) for t in self.all_tables]
            if not st.session_state.get("tables_selection_initialized"):
                st.session_state["selected_table_ids"] = list(table_ids)
                st.session_state["tables_selection_initialized"] = True
            selected_set = set(st.session_state.get("selected_table_ids", []))
            updated_selected_ids: list[str] = []

            for index, table in enumerate(self.all_tables):
                table_id = table_ids[index]
                checked_default = table_id in selected_set
                checked_current = st.session_state.get(
                    f"table_select_{index}", checked_default
                )
                header_cols = st.columns([0.03, 0.97])
                with header_cols[0]:
                    checked = st.checkbox(
                        "Use",
                        key=f"table_select_{index}",
                        value=checked_current,
                        label_visibility="collapsed",
                    )
                with header_cols[1]:
                    label = f"{table.name}"
                    with st.expander(label, expanded=False):
                        st.markdown(
                            table_meta_html("catalog", table.catalog),
                            unsafe_allow_html=True,
                        )
                        st.markdown(
                            table_meta_html("db", table.database),
                            unsafe_allow_html=True,
                        )
                        if table.description:
                            desc_html = self._format_table_description(table.description)
                            st.markdown(
                                table_desc_html(desc_html),
                                unsafe_allow_html=True,
                            )
                        if isinstance(table.schema, dict) and table.schema:
                            cols = "\n".join(
                                f"- `{col}` ({dtype})"
                                for col, dtype in table.schema.items()
                            )
                            st.markdown(cols)
                        else:
                            st.markdown("_Schema not available_")
                if checked:
                    updated_selected_ids.append(table_id)

            if not updated_selected_ids:
                st.warning("No tables selected. SQL generation will not work.")

            st.session_state["selected_table_ids"] = list(updated_selected_ids)
            selected_tables = [
                table
                for table in self.all_tables
                if self._table_id(table) in updated_selected_ids
            ]
            self.llm.tables = selected_tables
            self.llm.backend.tables = selected_tables

    def _render_steps_taken(self):
        steps = st.session_state.get("steps_taken", [])
        if not steps:
            return
        if not render_sidebar_steps(steps):
            for step in steps:
                st.sidebar.markdown(step)

    @staticmethod
    def _table_id(table) -> str:
        return f"{table.catalog}.{table.database}.{table.name}"

    def _build_previous_conversation_context(
        self, user_question: str, limit: int = 5
    ) -> str:
        runs = st.session_state.get("query_runs", [])
        entries = []
        final_message = f"\nNEXT QUESTION TO ANSWER: {user_question}\n"
        if not runs:
            return final_message
        for run in runs:
            if run.get("status") != "complete":
                continue
            question = run.get("question")
            sql = run.get("sql_query")
            answer = run.get("answer")
            if not question:
                continue
            if sql or answer:
                entries.append((question, sql, answer))
        if not entries:
            return final_message
        entries = entries[-limit:]
        lines = ["\nPREVIOUS CONVERSATION (oldest first)"]
        for idx, (question, sql, answer) in enumerate(entries, start=1):
            lines.append(f"\n{idx}. USER: {question}")
            if sql:
                lines.append(f"   SQL: {sql}")
            if answer:
                lines.append(f"   BOT: {answer}")
        lines.append(final_message)
        return "\n".join(lines)

    @staticmethod
    def _format_table_description(text: str) -> str:
        escaped = html.escape(text)
        parts = escaped.split("`")
        for i in range(1, len(parts), 2):
            parts[i] = f"<code>{parts[i]}</code>"
        return "".join(parts)


if __name__ == "__main__":
    app_choice = os.getenv("SQL_AI_STREAMLIT_APP", "cem").lower()
    if app_choice == "pixar":
        question = "avg length of film?"
        CB = ChatbotApp(
            athena_llm=PixarLLM,
            title="Pixar",
            default_question=question,
        )
    else:
        question = (
            "on avg how many trains stop at peterborough each day over the last week?"
        )
        CB = ChatbotApp(
            athena_llm=CEMLLM,
            title="CEM Timetable",
            default_question=question,
        )

    CB.run()
