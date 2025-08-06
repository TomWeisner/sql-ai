from typing import Callable, Optional, Union
from contextlib import contextmanager
from functools import wraps

from sql_ai.tracking.step import (
    Step,
    log_step_starting,
    log_unlogged_steps,
)
from sql_ai.tracking.tracker import (
    step_tracker,
)

from sql_ai.streamlit.utils import sidebar_typewriter


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
