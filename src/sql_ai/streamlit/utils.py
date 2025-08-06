import json
import re
import sys
import time
import traceback
from contextlib import contextmanager
from functools import wraps
from typing import Callable, Union

import streamlit as st

from sql_ai.tracking.decorator import (
    resolve_step_name,
)
from sql_ai.tracking.step import (
    Step,
    log_step_starting,
    log_unlogged_steps,
)
from sql_ai.tracking.tracker import (
    step_tracker,
)


def neat_prompt(prompt: dict) -> str:
    if not prompt:
        return ""
    prompt = prompt.copy()
    message = prompt["messages"][0]["content"]
    prompt.pop("messages")
    prompt["message"] = "[...]"
    body_str = json.dumps(prompt, indent=2)
    return f"{body_str}\nMessage:\n{message}\n"


def sidebar_typewriter(text: str, speed: float = 0.005):
    container = st.sidebar.empty()
    typed = ""
    for char in text:
        typed += char
        container.markdown(f"{typed}")
        time.sleep(speed)


def print_message(
    st,
    message: str,
    role: str = "system",
    should_remember: bool = False,
) -> float:
    assert role in ["system", "user", "assistant"]
    time_now = time.time()
    msg = {"role": role, "content": message}
    if should_remember:
        st.session_state.chat_history.append(msg)
    if role == "system":
        sidebar_typewriter(message)
    else:
        st.chat_message(role).markdown(msg["content"])
    return time_now


def display_enhanced_traceback(
    e: Exception,
    user_message: str = "An error occurred.",
    project_identifier: str = "sql_ai.",
):
    # 1. Get traceback and format it
    tb = traceback.extract_tb(sys.exc_info()[2])
    formatted_trace = traceback.format_exc()

    highlighted_trace = formatted_trace

    # 2. Highlight the last frame from your project
    user_frame = next(
        (frame for frame in reversed(tb) if project_identifier in frame.filename),
        None,
    )

    if user_frame:
        frame_info = (
            f'File "{user_frame.filename}", line {user_frame.lineno},'
            f" in {user_frame.name}"
        )
        highlighted_trace = re.sub(
            re.escape(frame_info),
            f"<b>{frame_info}</b>",
            highlighted_trace,
        )
        if user_frame.line:
            line_pattern = re.escape(user_frame.line.strip())
            highlighted_trace = re.sub(
                line_pattern,
                f"<b>{user_frame.line.strip()}</b>",
                highlighted_trace,
            )

    # 3. Highlight root exception message (last line of the trace)
    # Extract clean root exception (last line)
    tbe = traceback.TracebackException.from_exception(e)
    exception_only = "".join(
        tbe.format_exception_only()
    ).strip()  # e.g. "TypeError: something bad"
    highlighted_trace = highlighted_trace.replace(
        exception_only, f"<b>{exception_only}</b>"
    )
    # 4. Show user-facing error and expandable details
    st.error(user_message)

    with st.expander("Show full error details"):
        st.markdown(
            f"<pre style='color:red'>{highlighted_trace}</pre>",
            unsafe_allow_html=True,
        )


@contextmanager
def track_step_and_log_cm(start_message: Union[str, Callable], end_message: str = ""):
    resolved_name = resolve_step_name(start_message)
    step = Step(start_msg="▶️  " + resolved_name)
    step_tracker.push(step)
    speed = 0.001
    if step.level == 1:
        speed = 0
    sidebar_typewriter(text=log_step_starting(step), speed=speed)
    success = True
    try:
        yield
    except Exception:
        success = False
        raise
    finally:
        step.timer.stop_timer()
        step_tracker.pop()
        emoji = "✅" if success else "❌"
        if not end_message:
            end_message = f"{emoji} " + resolved_name
        step.end_msg = end_message
        log_lines = log_unlogged_steps(step)
        for line in log_lines:
            sidebar_typewriter(text=line, speed=0.001)


def track_step_and_log(start_message: Union[str, Callable], end_message: str = ""):
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            resolved_name = resolve_step_name(start_message, args=args, kwargs=kwargs)
            step = Step(start_msg="▶️  " + resolved_name)
            step_tracker.push(step)
            speed = 0.001
            if step.level == 1:
                speed = 0
            sidebar_typewriter(text=log_step_starting(step), speed=speed)
            success = True
            try:
                result = fn(*args, **kwargs)
                # Detect success flag in last return item (optional pattern)
                if isinstance(result, tuple) and isinstance(result[-1], bool):
                    *output_values, success = result
            except Exception:
                success = False
                raise

            finally:
                step.timer.stop_timer()
                step_tracker.pop()
                emoji = "✅" if success else "❌"
                final_msg = end_message or f"{emoji} {resolved_name}"
                step.end_msg = final_msg
                log_lines = log_unlogged_steps(step)
                for line in log_lines:
                    sidebar_typewriter(text=line, speed=0.001)
            return result

        return wrapper

    return decorator
