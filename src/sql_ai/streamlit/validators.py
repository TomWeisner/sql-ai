"""Validation helpers for Streamlit UI state."""

from __future__ import annotations

from typing import Iterable

from sql_ai.sql_backends.table import Table


def get_storage_platform_error(tables: Iterable[Table]) -> str | None:
    platforms = [
        (getattr(table, "storage_platform", None) or "unknown").lower()
        for table in tables
    ]
    if not platforms:
        return None
    unique_platforms = sorted(set(platforms))
    if len(unique_platforms) <= 1:
        return None
    pretty = ", ".join(p.title() for p in unique_platforms)
    return (
        "Please select tables that share the same storage platform. "
        f"Selected platforms: {pretty}."
    )
