import streamlit


def st_if_ctx():
    """
    Return the `streamlit` module if a ScriptRunContext exists; else None.
    Works across Streamlit versions.
    """
    try:
        try:
            # Newer path
            from streamlit.runtime.scriptrunner import get_script_run_ctx
        except Exception:
            from streamlit.runtime.scriptrunner_utils import (
                script_run_context as _src,  # type: ignore
            )

            get_script_run_ctx = _src.get_script_run_ctx  # type: ignore

        return streamlit if get_script_run_ctx() is not None else None
    except Exception:
        return None
