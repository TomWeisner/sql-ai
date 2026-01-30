"""Formatting helpers for table metadata in the Streamlit UI."""

from __future__ import annotations

import html


def format_table_description(text: str) -> str:
    escaped = html.escape(text)
    parts = escaped.split("`")
    for i in range(1, len(parts), 2):
        parts[i] = f"<code>{parts[i]}</code>"
    return "".join(parts)
