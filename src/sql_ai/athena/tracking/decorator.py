from typing import Callable, Optional, Union


def resolve_step_name(
    name: Union[str, Callable],
    args: Optional[tuple] = None,
    kwargs: Optional[dict] = None,
) -> str:
    if callable(name):
        try:
            return name(*(args or []), **(kwargs or {}))
        except Exception as e:
            return f"<error evaluating step name: {e}>"
    return name
