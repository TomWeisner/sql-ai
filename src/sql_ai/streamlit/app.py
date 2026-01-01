"""
Streamlit app for running a chatbot with Bedrock.

This app uses our SQL LLM class to generate SQL queries.

Run the app with the below from the project root:
streamlit run src/sql_ai/streamlit/app.py
"""

import logging
import os
from datetime import datetime

import pandas as pd
import streamlit as st

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
    print_message,
)
from sql_ai.tracking.decorator import track_step_and_log, track_step_and_log_cm

# Suppress Streamlit-specific warnings/logs
logging.getLogger("streamlit").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.scriptruncontext").setLevel(logging.ERROR)


class ChatbotApp:
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
        }
        for k, v in defaults.items():
            st.session_state.setdefault(k, v)

    def run(self):
        print("Running app...")
        set_sidebar_width_and_center_content(sidebar_width=450, max_content_width=1100)
        set_title_top_padding(rem=0)
        st.sidebar.title("🧭 Steps taken")
        st.title(f"🚂 LNER LLMs - {self.title}")

        keep_context = st.checkbox("Keep chat memory", value=True)
        use_supplied_sql = st.checkbox(
            "Use a supplied SQL query", value=False, key="use_supplied_sql"
        )

        submitted, user_input = self._input_form()
        question = user_input if submitted else None

        if submitted and question:
            self._handle_question(question, keep_context, use_supplied_sql)
            self._render_tabs()
            self._show_answer(keep_context)

        self._render_buttons()

    def _input_form(self):
        if st.session_state.retry_triggered:
            st.session_state.retry_triggered = False
            return True, st.session_state.last_user_input

        with st.form("chat_form", clear_on_submit=True):
            user_input = st.text_input(
                "Ask a question about the data:",
                value=st.session_state.get("default_question", ""),
            )
            submitted = st.form_submit_button("Send")
        if submitted and user_input:
            st.session_state.last_user_input = user_input
        return submitted, user_input

    def _handle_question_actual(self, question, use_supplied_sql):
        try:
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

            st.session_state.update(
                {
                    "sql_query": sql_result.sql,
                    "format_logs": "\n".join(sql_result.format_logs),
                    "error_traceback": sql_result.error_traceback,
                }
            )

            if sql_result.error_traceback:
                st.error(sql_result.error_traceback)

            with track_step_and_log_cm(
                f"⚙️ Running SQL query on {self.llm.backend.name}..."
            ):
                df = self.llm.run_query(sql_result.sql)
                st.session_state.results_df = df

            with track_step_and_log_cm("⏳ Generating answer..."):
                answer, data_prompt = self.llm.question_about_data(question, df)
                st.session_state.update(
                    {
                        "answer": answer,
                        "data_prompt": neat_prompt(data_prompt),
                    }
                )
                st.session_state.query_runs.append(
                    {
                        "question": question,
                        "sql_query": st.session_state.sql_query,
                        "sql_prompt": st.session_state.sql_prompt,
                        "format_logs": st.session_state.format_logs,
                        "results_df": df.copy() if isinstance(df, pd.DataFrame) else df,
                        "data_prompt": st.session_state.data_prompt,
                    }
                )
        except Exception as e:
            display_enhanced_traceback(e)

    def _clear_previous_variables_and_rewrite_messages(self, question, keep_context):
        for k in [
            "answer",
            "results_df",
            "format_logs",
            "data_prompt",
            "sql_prompt",
            "sql_query",
        ]:
            st.session_state[k] = None
        for chat in st.session_state.chat_history:
            if chat["role"] != "system":
                print_message(st, chat["content"], chat["role"], should_remember=False)
        print_message(st, question, role="user", should_remember=keep_context)

    @track_step_and_log("**Processing user input**")
    def _handle_question(self, question, keep_context, use_supplied_sql):
        self._clear_previous_variables_and_rewrite_messages(question, keep_context)
        with st.spinner(f"Generating answer... ({self.llm.config.bedrock_model.name})"):
            self._handle_question_actual(question, use_supplied_sql)

    def _render_tabs(self):
        runs = st.session_state.get("query_runs", [])
        for idx, run in enumerate(runs):
            label = f"Details for: {run.get('question','(unknown)')}"
            with st.expander(label, expanded=(idx == len(runs) - 1)):
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
                    tabs.append(
                        (
                            "⬇️ Download data",
                            lambda csv=csv, file_name=file_name: st.download_button(
                                "⬇️ Download output data.", csv, file_name, "text/csv"
                            ),
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

    def _show_answer(self, keep_context):
        if answer := st.session_state.answer:
            if st.session_state.error_traceback:
                st.warning(
                    "The below answer may have been generated from a malformed SQL query."
                )
            print_message(st, answer, role="assistant", should_remember=keep_context)

    def _render_buttons(self):
        if st.session_state.last_user_input:
            col1, _, col2 = st.columns([1, 2, 1])
            with col1:
                if st.button("🧹 Clear chat"):
                    for key in list(st.session_state.keys()):
                        del st.session_state[key]
                    st.session_state["suppress_default_question"] = True
                    st.session_state["query_runs"] = []
                    st.rerun()
            with col2:
                if st.button("🔁 Retry question"):
                    st.session_state.retry_triggered = True
                    st.rerun()


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
