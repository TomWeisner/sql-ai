"""Prompt-context helpers for the Streamlit app."""

from __future__ import annotations

from typing import Iterable


def build_previous_conversation_context(
    user_question: str,
    runs: Iterable[dict],
    limit: int = 5,
    include_question: bool = True,
) -> str:
    entries = []
    final_message = (
        f"\nNEXT QUESTION TO ANSWER: {user_question}\n" if include_question else ""
    )
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
            lines.append(f"\n   SQL: {sql}")
        if answer:
            lines.append(f"\n   BOT: {answer}")
    lines.append(final_message)
    return "\n".join(lines)
