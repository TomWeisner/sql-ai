"""Sidebar helpers for rendering step progress."""

from __future__ import annotations

import streamlit as st
from sql_ai.streamlit.utils import render_sidebar_steps, set_sidebar_steps_placeholder


def init_steps_sidebar() -> None:
    st.sidebar.title("🧭 Steps taken")
    set_sidebar_steps_placeholder(st.sidebar.empty())


def render_steps_taken(steps: list[str]) -> None:
    if not steps:
        return
    if not render_sidebar_steps(steps):
        for step in steps:
            st.sidebar.markdown(step)
