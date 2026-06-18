"""Streamlit action handlers for running queries and updating state."""

from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd

import streamlit as st
from sql_ai.sql_llm import SqlLLM
from sql_ai.streamlit.aws_auth import build_aws_login_message, is_aws_auth_error
from sql_ai.streamlit.context_prompt import build_previous_conversation_context
from sql_ai.streamlit.query_controls import QueryControls
from sql_ai.streamlit.sql_utils import normalize_sql
from sql_ai.streamlit.utils import display_enhanced_traceback, neat_prompt
from sql_ai.streamlit.validators import get_storage_platform_error
from sql_ai.tracking.decorator import track_step_and_log_cm


def clear_previous_variables() -> None:
    for key in [
        "answer",
        "results_df",
        "format_logs",
        "data_prompt",
        "sql_prompt",
        "sql_query",
    ]:
        st.session_state[key] = None


def set_llm_extra_context(
    llm: SqlLLM, question: str, include_question: bool = True
) -> None:
    if st.session_state.get("keep_context", True):
        llm.sql_prompt.extra_context = build_previous_conversation_context(
            question,
            st.session_state.get("query_runs", []),
            include_question=include_question,
        )
    else:
        llm.sql_prompt.extra_context = ""


def handle_question(
    question: str,
    keep_context: bool,
    use_supplied_sql: bool,
    dry_run: bool = False,
    interpret_with_llm: bool = True,
) -> None:
    clear_previous_variables()
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


def process_question(
    llm: SqlLLM,
    question: str,
    controls: QueryControls,
) -> bool:
    auth_notice = st.session_state.get("aws_auth_notice")
    if auth_notice:
        st.error(auth_notice)
        return False
    if not llm.tables:
        st.error("Please select at least one table to query.")
        return False
    platform_error = get_storage_platform_error(llm.tables)
    if platform_error:
        st.error(platform_error)
        return False
    handle_question(
        question,
        controls.keep_context,
        controls.use_supplied_sql,
        controls.dry_run,
        controls.interpret_with_llm,
    )
    return True


def handle_question_actual(
    llm: SqlLLM,
    question: str,
    use_supplied_sql: bool,
    dry_run: bool = False,
    interpret_with_llm: bool = True,
) -> Optional[dict]:
    start_time = datetime.now()
    try:
        if use_supplied_sql:
            with track_step_and_log_cm("📥 Using user-supplied SQL..."):
                sql_result = llm.get_sql(question, use_supplied_sql=use_supplied_sql)
        else:
            set_llm_extra_context(llm, question, include_question=True)
            with track_step_and_log_cm("🧠 Converting natural language to SQL..."):
                sql_result = llm.get_sql(question, use_supplied_sql=use_supplied_sql)
                prompt_body = sql_result.prompt_body or {}
                st.session_state.update({"sql_prompt": neat_prompt(dict(prompt_body))})

        normalized_sql = normalize_sql(llm, sql_result.sql)
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

        with track_step_and_log_cm(f"⚙️ Running SQL query on {llm.backend.name}..."):
            df = llm.run_query(normalized_sql)
            st.session_state.results_df = df

        if interpret_with_llm:
            with track_step_and_log_cm("⏳ Interpreting data..."):
                set_llm_extra_context(llm, question, include_question=False)
                answer, data_prompt = llm.question_about_data(question, df)
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
                    "answer_source": "interpret",
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
    except Exception as exc:
        error_details = display_enhanced_traceback(exc)
        duration_s = (datetime.now() - start_time).total_seconds()
        is_auth_error = is_aws_auth_error(exc)
        error_message = (
            build_aws_login_message(llm.config.aws_profile, exc)
            if is_auth_error
            else (
                error_details.get("message")
                if isinstance(error_details, dict)
                else "An error occurred."
            )
        )
        if is_auth_error:
            st.session_state["aws_auth_notice"] = error_message
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
            "error_message": error_message,
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


def execute_saved_sql(
    llm: SqlLLM, idx: int, run: dict, generate_answer: bool = False
) -> None:
    sql_query = run.get("sql_query")
    if not sql_query:
        return
    question = run.get("question", "")
    start_time = datetime.now()
    try:
        with track_step_and_log_cm(f"⚙️ Running SQL query on {llm.backend.name}..."):
            df = llm.run_query(sql_query)
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
                set_llm_extra_context(llm, question, include_question=False)
                answer, data_prompt = llm.question_about_data(question, df)
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
                    "answer_source": "interpret",
                    "ran_sql_only": False,
                }
            )

        st.session_state.query_runs[idx] = update_payload
    except Exception as exc:
        error_details = display_enhanced_traceback(exc)
        error_message = (
            build_aws_login_message(llm.config.aws_profile, exc)
            if is_aws_auth_error(exc)
            else (
                error_details.get("message")
                if isinstance(error_details, dict)
                else "An error occurred."
            )
        )
        if is_aws_auth_error(exc):
            st.session_state["aws_auth_notice"] = error_message
        st.session_state.query_runs[idx] = {
            **run,
            "status": "error",
            "error_message": error_message,
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


def interpret_saved_result(llm: SqlLLM, idx: int, run: dict) -> None:
    df = run.get("results_df")
    if df is None:
        df = st.session_state.get("results_df")
    if df is None:
        return
    question = run.get("question", "")
    start_time = datetime.now()
    try:
        with track_step_and_log_cm("⏳ Generating answer..."):
            set_llm_extra_context(llm, question, include_question=False)
            answer, data_prompt = llm.question_about_data(question, df)
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
            "answer_source": "interpret",
            "ran_sql_only": False,
            "status": "complete",
            "duration_s": (datetime.now() - start_time).total_seconds(),
            "answered_at": datetime.now().strftime("%H:%M:%S"),
        }
    except Exception as exc:
        error_details = display_enhanced_traceback(exc)
        error_message = (
            build_aws_login_message(llm.config.aws_profile, exc)
            if is_aws_auth_error(exc)
            else (
                error_details.get("message")
                if isinstance(error_details, dict)
                else "An error occurred."
            )
        )
        if is_aws_auth_error(exc):
            st.session_state["aws_auth_notice"] = error_message
        st.session_state.query_runs[idx] = {
            **run,
            "status": "error",
            "error_message": error_message,
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
