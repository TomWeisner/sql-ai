"""Sidebar panel for selecting Bedrock models."""

from __future__ import annotations

from typing import cast

import streamlit as st
from sql_ai.bedrock.models import MODEL_REGISTRY
from sql_ai.config import ModelKey
from sql_ai.sql_llm import SqlLLM


def apply_model_selection(llm: SqlLLM, model_key: ModelKey) -> None:
    if model_key not in MODEL_REGISTRY:
        return
    llm.bedrock_config.set_model_key(model_key)
    llm.sql_prompt.model = llm.bedrock_config.model


def render_models_panel(llm: SqlLLM) -> None:
    with st.sidebar.expander("🧠 Available models", expanded=False):
        model_keys = list(MODEL_REGISTRY.keys())
        if not model_keys:
            st.markdown("_No models available_")
            return
        current_key = st.session_state.get("model_key", llm.bedrock_config.model_key)
        if current_key not in MODEL_REGISTRY:
            current_key = model_keys[0]
            st.session_state["model_key"] = current_key
        selected_key = cast(
            ModelKey,
            st.radio(
                "Model",
                model_keys,
                key="model_key",
                format_func=lambda k: f"{MODEL_REGISTRY[k].name}",
                label_visibility="collapsed",
            ),
        )
        if selected_key != llm.bedrock_config.model_key:
            apply_model_selection(llm, selected_key)
