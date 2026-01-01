from __future__ import annotations

import os

try:
    import streamlit  # type: ignore
except ModuleNotFoundError:  # Streamlit is optional outside the UI.
    streamlit = None  # type: ignore


def st_if_ctx():
    """
    Return the `streamlit` module if a ScriptRunContext exists; else None.
    Works across Streamlit versions.
    """
    if os.getenv("SQL_AI_CLI_MODE") == "1":
        return None
    try:
        try:
            # Newer path
            from streamlit.runtime.scriptrunner import get_script_run_ctx
        except Exception:
            from streamlit.runtime.scriptrunner_utils import (
                script_run_context as _src,  # type: ignore
            )

            get_script_run_ctx = _src.get_script_run_ctx  # type: ignore

        if streamlit is None:
            return None
        return streamlit if get_script_run_ctx() is not None else None
    except Exception:
        return None
