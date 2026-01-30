"""
Streamlit app for running a chatbot with Bedrock.

This app uses our SQL LLM class to generate SQL queries.

Run the app with the below from the project root:
streamlit run src/sql_ai/streamlit/entrypoint.py
"""

import logging

import streamlit as st
from sql_ai.sql_llm import SqlLLM
from sql_ai.streamlit.answer_footer_controls import render_footer_controls
from sql_ai.streamlit.conversation import render_conversation
from sql_ai.streamlit.css_utils import (
    inject_app_styles,
    set_sidebar_width_and_center_content,
    set_title_top_padding,
)
from sql_ai.streamlit.handlers import (
    execute_saved_sql,
    handle_question_actual,
    interpret_saved_result,
    process_question,
)
from sql_ai.streamlit.query_controls import QueryControls, render_query_controls
from sql_ai.streamlit.sidebar import render_sidebar
from sql_ai.streamlit.state import get_question, init_session_state
from sql_ai.streamlit.table_formatting import format_table_description

# Suppress Streamlit-specific warnings/logs
logging.getLogger("streamlit").setLevel(logging.ERROR)
logging.getLogger("streamlit.runtime.scriptruncontext").setLevel(logging.ERROR)


class ChatbotApp:
    QUESTION_INPUT_LABEL = "Ask a question about the data"

    def __init__(self, athena_llm: SqlLLM, title: str, default_question: str = ""):
        self.llm = athena_llm
        saved_tables = st.session_state.get("all_tables")
        if saved_tables:
            self.all_tables = list(saved_tables)
        else:
            base_tables = self.llm.backend.tables or self.llm.tables
            self.all_tables = list(base_tables)
            st.session_state["all_tables"] = list(self.all_tables)
        self.title = title
        self.default_question = default_question
        init_session_state(self.default_question, self.llm)

    def run(self):
        self._setup_layout()
        self._render_sidebar()
        self._render_header()
        controls = render_query_controls()
        self._render_main(controls)

    def _setup_layout(self) -> None:
        st.session_state.setdefault("suppress_sidebar_typewriter", True)
        set_sidebar_width_and_center_content(sidebar_width=450, max_content_width=1100)
        set_title_top_padding(rem=0.5)
        inject_app_styles(chat_width=640)

    def _render_sidebar(self) -> None:
        self.all_tables = render_sidebar(
            self.llm, self.all_tables, format_table_description
        )

    def _render_header(self) -> None:
        st.markdown("<div id='page-top'></div>", unsafe_allow_html=True)
        st.title(f"🚂 LNER LLMs - {self.title}")

    def _process_question(self, controls: QueryControls) -> bool:
        question = get_question(self.QUESTION_INPUT_LABEL)
        if not question:
            return False
        return process_question(self.llm, question, controls)

    def _render_main(self, controls: QueryControls) -> None:
        render_conversation(
            llm=self.llm,
            handle_question_actual=lambda q, s, d, i: handle_question_actual(
                self.llm, q, s, d, i
            ),
            execute_saved_sql=lambda idx, run, gen: execute_saved_sql(
                self.llm, idx, run, gen
            ),
            interpret_saved_result=lambda idx, run: interpret_saved_result(
                self.llm, idx, run
            ),
        )
        render_footer_controls(
            execute_saved_sql=lambda idx, run, gen: execute_saved_sql(
                self.llm, idx, run, gen
            ),
            interpret_saved_result=lambda idx, run: interpret_saved_result(
                self.llm, idx, run
            ),
        )
        if self._process_question(controls):
            st.rerun()
