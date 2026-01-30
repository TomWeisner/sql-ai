"""Sidebar panel for table selection and schema display."""

from __future__ import annotations

import streamlit as st

from sql_ai.sql_backend.table import Table
from sql_ai.sql_llm import SqlLLM
from sql_ai.streamlit.ui_templates import table_desc_html, table_meta_html


def _table_id(table: Table) -> str:
    return f"{table.catalog}.{table.database}.{table.name}"


def render_tables_panel(
    llm: SqlLLM,
    all_tables: list[Table],
    format_table_description,
) -> list[Table]:
    with st.sidebar.expander("📚 Available tables", expanded=False):
        if not all_tables:
            st.markdown("_No tables available_")
            return all_tables

        if not st.session_state.get("tables_loaded"):
            with st.spinner("Loading table schemas..."):
                try:
                    previous_typewriter = st.session_state.get(
                        "suppress_sidebar_typewriter", True
                    )
                    st.session_state["suppress_sidebar_typewriter"] = True
                    original_tables = llm.backend.tables
                    llm.backend.tables = all_tables
                    llm.backend.populate_schemas()
                    st.session_state.tables_loaded = True
                except Exception as e:
                    st.warning(f"Could not load table schemas: {e}")
                finally:
                    llm.backend.tables = original_tables
                    st.session_state["suppress_sidebar_typewriter"] = previous_typewriter

        unique_tables: list[Table] = []
        seen_ids = set()
        for table in all_tables:
            table_id = _table_id(table)
            if table_id in seen_ids:
                continue
            seen_ids.add(table_id)
            unique_tables.append(table)
        all_tables = unique_tables
        table_ids = [_table_id(t) for t in all_tables]
        st.session_state["all_tables"] = list(all_tables)
        if not st.session_state.get("tables_selection_initialized"):
            st.session_state["selected_table_ids"] = list(table_ids)
            st.session_state["tables_selection_initialized"] = True
        selected_set = set(st.session_state.get("selected_table_ids", []))
        updated_selected_ids: list[str] = []

        for index, table in enumerate(all_tables):
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
                    platform = table.storage_platform or "unknown"
                    st.markdown(
                        table_meta_html("Platform", platform),
                        unsafe_allow_html=True,
                    )
                    if platform.lower() == "athena":
                        st.markdown(
                            table_meta_html("Catalog", table.catalog or ""),
                            unsafe_allow_html=True,
                        )
                    st.markdown(
                        table_meta_html("Database", table.database or ""),
                        unsafe_allow_html=True,
                    )
                    if table.description:
                        desc_html = format_table_description(table.description)
                        st.markdown(
                            table_desc_html(desc_html),
                            unsafe_allow_html=True,
                        )
                    if isinstance(table.schema, dict) and table.schema:
                        cols = "\n".join(
                            f"- `{col}` ({dtype})" for col, dtype in table.schema.items()
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
            table for table in all_tables if _table_id(table) in updated_selected_ids
        ]
        llm.tables = selected_tables
        llm.backend.tables = selected_tables

    return all_tables
