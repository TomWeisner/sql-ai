"""
Streamlit app for running a chatbot with Bedrock.

This app uses our SQL LLM class to generate SQL queries.

Run the app with the below from the project root:
streamlit run src/sql_ai/streamlit/app.py
"""

import html
import logging
import os
from datetime import datetime

import pandas as pd
import streamlit as st
import streamlit.components.v1 as components

from sql_ai.app_objects.cem_timetable import CEMLLM
from sql_ai.app_objects.pixar_films import PixarLLM
from sql_ai.sql_llm import SqlLLM
from sql_ai.streamlit.css_utils import (
    set_sidebar_width_and_center_content,
    set_title_top_padding,
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
        }
        for k, v in defaults.items():
            st.session_state.setdefault(k, v)

    def run(self):
        print("Running app...")
        st.session_state.setdefault("suppress_sidebar_typewriter", True)
        set_sidebar_width_and_center_content(sidebar_width=450, max_content_width=1100)
        set_title_top_padding(rem=0)
        st.markdown(
            """
            <style>
            :root {
                --chat-width: 640px;
            }
            .stMarkdown,
            .stMarkdown > div {
                width: 100%;
            }
            .block-container {
                max-width: var(--chat-width);
            }
            .user-row {
                display: inline-flex;
                justify-content: flex-end;
                align-items: center;
                gap: 10px;
                background: #f6f7f9;
                border-radius: 14px;
                padding: 14px 16px;
                margin: 6px 0 12px 0;
                max-width: 100%;
            }
            .user-row-wrap {
                display: flex;
                justify-content: flex-end;
                width: 100%;
            }
            .user-row .user-text {
                text-align: right;
                font-weight: 500;
            }
            .chat-action-row {
                display: inline-flex;
                gap: 6px;
                align-items: center;
                width: 100%;
            }
            .message-time {
                font-size: 0.75rem;
                color: #9ca3af;
                margin-top: 4px;
            }
            .message-time.right {
                text-align: right;
            }
            .message-time.left {
                text-align: left;
            }
            [data-testid="stTextInput"] input {
                border-radius: 999px;
                padding: 0.65rem 0.95rem;
                border: 1px solid #e5e7eb;
                background: #f3f4f6;
                width: 100%;
            }
            div[data-testid="stChatInput"] {
                max-width: var(--chat-width);
                margin-left: auto;
                margin-right: auto;
            }
            div[data-testid="stChatInput"] > div {
                max-width: var(--chat-width);
                margin-left: auto;
                margin-right: auto;
            }
            [data-testid="stTextInput"] div[data-baseweb="base-input"] {
                background: transparent;
                border: none;
            }
            [data-testid="stTextInput"] div[data-baseweb="base-input"] > div {
                background: transparent;
            }
            [data-testid="stForm"] button {
                border-radius: 999px;
                padding: 0.55rem 0.8rem;
                border: 1px solid #e5e7eb;
                background: #f3f4f6;
            }
            [data-testid="stButton"] button {
                white-space: nowrap;
                width: 100%;
            }
            button.btn-clear,
            button.btn-retry {
                border: none;
                background: transparent;
                box-shadow: none;
                padding: 6px 8px;
                border-radius: 10px;
                font-size: 1rem;
                font-weight: 500;
                display: inline-flex;
                align-items: center;
                justify-content: center;
            }
            button.btn-clear:hover {
                background: #fee2e2;
                color: #991b1b;
            }
            button.btn-retry:hover {
                background: #dcfce7;
                color: #166534;
            }
            div[data-testid="stExpander"] {
                border: none;
                box-shadow: none;
                max-width: var(--chat-width);
                margin-left: 0;
            }
            div[data-testid="stExpander"] > details {
                border: none;
                box-shadow: none;
            }
            </style>
            """,
            unsafe_allow_html=True,
        )
        self._render_tables_panel()
        st.sidebar.title("🧭 Steps taken")
        set_sidebar_steps_placeholder(st.sidebar.empty())
        self._render_steps_taken()
        st.title(f"🚂 LNER LLMs - {self.title}")
        self._render_query_options()

        keep_context = st.session_state.get("keep_context", True)
        use_supplied_sql = st.session_state.get("use_supplied_sql", False)
        dry_run = st.session_state.get("dry_run", True)

        question = self._get_question()

        if question:
            self._handle_question(question, keep_context, use_supplied_sql, dry_run)

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

    def _handle_question_actual(self, question, use_supplied_sql, dry_run=False):
        try:
            start_time = datetime.now()

            if use_supplied_sql:
                with track_step_and_log_cm("📥 Using user-supplied SQL..."):
                    sql_result = self.llm.get_sql(
                        question, use_supplied_sql=use_supplied_sql
                    )

            else:
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
                    "results_df": None,
                    "data_prompt": None,
                    "answered_at": datetime.now().strftime("%H:%M:%S"),
                    "answer": None,
                    "dry_run": True,
                    "duration_s": (datetime.now() - start_time).total_seconds(),
                }

            with track_step_and_log_cm(
                f"⚙️ Running SQL query on {self.llm.backend.name}..."
            ):
                df = self.llm.run_query(normalized_sql)
                st.session_state.results_df = df

            with track_step_and_log_cm("⏳ Generating answer..."):
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
                    "results_df": df.copy() if isinstance(df, pd.DataFrame) else df,
                    "data_prompt": st.session_state.data_prompt,
                    "answered_at": datetime.now().strftime("%H:%M:%S"),
                    "answer": answer,
                    "dry_run": False,
                    "duration_s": (datetime.now() - start_time).total_seconds(),
                }
        except Exception as e:
            display_enhanced_traceback(e)
            return None

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

    def _handle_question(self, question, keep_context, use_supplied_sql, dry_run=False):
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
                "asked_at": asked_at,
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
                        with st.spinner(
                            f"Generating answer... ({self.llm.config.bedrock_model.name})"
                        ):
                            with track_step_and_log_cm("Processing user input"):
                                result = self._handle_question_actual(
                                    run.get("question", ""),
                                    run.get("use_supplied_sql", False),
                                    run.get("dry_run", False),
                                )
                    finally:
                        st.session_state["suppress_sidebar_typewriter"] = (
                            previous_typewriter
                        )
                if result:
                    st.session_state.query_runs[idx] = {
                        **run,
                        **result,
                        "status": "complete",
                    }
                else:
                    st.session_state.query_runs[idx]["status"] = "error"
                st.rerun()
                return

            if run.get("answer"):
                has_answer = True
            else:
                has_answer = False

            with st.expander("Details", expanded=False):
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
                                df, use_container_width=True, num_rows="dynamic"
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

            answered_at = run.get("answered_at")
            if has_answer:
                self._render_assistant_message(run["answer"], answered_at)
            elif run.get("dry_run"):
                st.caption("Dry run: SQL generated, not executed.")
                if answered_at:
                    st.markdown(
                        f"<div class='message-time left'>{answered_at}</div>",
                        unsafe_allow_html=True,
                    )

    def _render_user_message(self, question: str, timestamp: str | None):
        question_text = html.escape(question)
        st.markdown(
            f"""
            <div class="user-row-wrap">
                <div class="user-row">
                    <span class="user-text">{question_text}</span>
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        if timestamp:
            st.markdown(
                f"<div class='message-time right'>{timestamp}</div>",
                unsafe_allow_html=True,
            )

    def _render_assistant_message(self, message: str, timestamp: str | None):
        st.markdown(message)
        if timestamp:
            st.markdown(
                f"<div class='message-time left'>{timestamp}</div>",
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
            col_retry, col_clear, _ = st.columns([0.5, 0.5, 9], gap="small")
            with col_retry:
                if st.button("🔁", help="Retry last question"):
                    st.session_state.retry_triggered = True
                    st.rerun()
            with col_clear:
                if st.button("🧹", help="Clear chat"):
                    for key in list(st.session_state.keys()):
                        del st.session_state[key]
                    st.session_state["suppress_default_question"] = True
                    st.session_state["query_runs"] = []
                    st.rerun()
            components.html(
                """
                <script>
                const root = window.parent.document;
                const buttons = root.querySelectorAll('button');
                buttons.forEach((btn) => {
                  const text = (btn.innerText || '').trim().toLowerCase();
                  if (text === '🧹') {
                    btn.classList.add('btn-clear');
                  }
                  if (text === '🔁') {
                    btn.classList.add('btn-retry');
                  }
                });
                </script>
                """,
                height=0,
            )

    def _render_query_options(self):
        col_left, col_mid, col_right = st.columns([1, 1, 1])
        with col_left:
            st.checkbox(
                "Keep memory",
                key="keep_context",
                help=(
                    "When on, previous Q&A remain in chat history so "
                    "the model can use prior context."
                ),
            )
        with col_mid:
            st.checkbox(
                "Supplied SQL",
                key="use_supplied_sql",
                help=(
                    "If checked, you can paste SQL instead of generating "
                    "it from natural language."
                ),
            )
        with col_right:
            st.checkbox(
                "Dry run",
                key="dry_run",
                help="Generate SQL and formatting logs but skip executing the query.",
            )

    def _render_tables_panel(self):
        with st.sidebar.expander("📚 Available tables", expanded=False):
            if not st.session_state.get("tables_loaded"):
                with st.spinner("Loading table schemas..."):
                    try:
                        previous_typewriter = st.session_state.get(
                            "suppress_sidebar_typewriter", True
                        )
                        st.session_state["suppress_sidebar_typewriter"] = True
                        self.llm.backend.populate_schemas()
                        st.session_state.tables_loaded = True
                    except Exception as e:
                        st.warning(f"Could not load table schemas: {e}")
                    finally:
                        st.session_state["suppress_sidebar_typewriter"] = (
                            previous_typewriter
                        )
            tables = list(self.llm.tables)
            for index, table in enumerate(tables):
                st.markdown(
                    f"**{table.name}** — catalog={table.catalog}, db={table.database}"
                )
                if table.description:
                    st.caption(table.description)
                if isinstance(table.schema, dict) and table.schema:
                    cols = "\n".join(
                        f"- `{col}` ({dtype})" for col, dtype in table.schema.items()
                    )
                    st.markdown(cols)
                else:
                    st.markdown("_Schema not available_")
                if len(tables) > 1 and index < len(tables) - 1:
                    st.markdown("---")

    def _render_steps_taken(self):
        steps = st.session_state.get("steps_taken", [])
        if not steps:
            return
        if not render_sidebar_steps(steps):
            for step in steps:
                st.sidebar.markdown(step)


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
