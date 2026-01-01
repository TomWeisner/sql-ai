from contextlib import contextmanager
from functools import wraps
from typing import Callable, Optional, Union

from sql_ai.streamlit.safe import st_if_ctx
from sql_ai.streamlit.utils import render_sidebar_steps, sidebar_typewriter
from sql_ai.tracking.step import (
    Step,
    log_step_starting,
    log_unlogged_steps,
)
from sql_ai.tracking.tracker import (
    step_tracker,
)


def resolve_step_name(
    name: Union[str, Callable],
    args: Optional[tuple] = None,
    kwargs: Optional[dict] = None,
) -> str:
    """
    Resolve a step name, which can either be a string or a callable.

    If `name` is a callable, it is called with `args` and `kwargs`, and the
    result is returned as the resolved step name. If the callable raises an
    exception, the exception is caught and a string describing the exception is
    returned.

    If `name` is a string, it is returned unchanged.

    :param name: The step name to resolve, which can be a string or a callable.
    :param args: The arguments to pass to the callable, if `name` is a callable.
    :param kwargs: The keyword arguments to pass to the callable, if `name` is a callable.
    :return: The resolved step name, which is a string.
    """
    if callable(name):
        try:
            return name(*(args or []), **(kwargs or {}))
        except Exception as e:
            return f"<error evaluating step name: {e}>"
    return name


def _write_line(text: str, speed: float):
    print(text)
    st = st_if_ctx()
    if st is not None:
        if not st.session_state.get("suppress_steps"):
            steps = st.session_state.setdefault("steps_taken", [])
            steps.append(text)
            if not st.session_state.get("suppress_sidebar_typewriter"):
                sidebar_typewriter(text=text, speed=speed)
            else:
                render_sidebar_steps(steps)


@contextmanager
def track_step_and_log_cm(start_message: Union[str, Callable], end_message: str = ""):
    resolved_name = resolve_step_name(start_message)
    step = Step(start_msg="▶️  " + resolved_name)
    step_tracker.push(step)

    speed = 0.001
    if step.level == 1:
        speed = 0

    _write_line(log_step_starting(step), speed)

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
        step.end_msg = end_message or f"{emoji} {resolved_name}"
        for line in log_unlogged_steps(step):
            _write_line(line, 0.001)


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

            _write_line(log_step_starting(step), speed)
            success = True
            try:
                result = fn(*args, **kwargs)
                if isinstance(result, tuple) and isinstance(result[-1], bool):
                    *_, success = result
            except Exception:
                success = False
                raise
            finally:
                step.timer.stop_timer()
                step_tracker.pop()
                emoji = "✅" if success else "❌"
                step.end_msg = end_message or f"{emoji} {resolved_name}"
                for line in log_unlogged_steps(step):
                    _write_line(line, 0.001)
            return result

        return wrapper

    return decorator
