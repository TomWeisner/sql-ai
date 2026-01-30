"""SQL formatting helpers for the Streamlit app."""

from __future__ import annotations

from sql_ai.sql_llm import SqlLLM


def normalize_sql(llm: SqlLLM, sql: str) -> str:
    cleaned = sql.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.strip("`").strip()
        if cleaned.lower().startswith("sql"):
            cleaned = cleaned[3:].lstrip()
    return llm._clean_query_prefix(cleaned)
