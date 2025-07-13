from typing import Optional

from sql_ai.utils.utils import Timer


class Step:
    def __init__(
        self,
        start_msg: str,
        end_msg: Optional[str] = None,
        steps: Optional[dict[str, "Step"]] = None,
        logged_external: bool = False,
        level: int = 0,
    ):
        self.start_msg = start_msg
        self.end_msg = end_msg
        self.timer = Timer()
        self.steps: dict[str, Step] = steps or {}
        self.logged_external = logged_external
        self.level = level

    def add_step(self, step: "Step") -> None:
        self.steps[step.start_msg] = step

    def _repr(self, indent: int = 0) -> str:
        indent_str = "  " * indent
        rep = f"{indent_str}{self.start_msg}: Step(duration={self.timer.elapsed})"
        if self.steps:
            for substep in self.steps.values():
                rep += f"\n{substep._repr(indent + 1)}"
        return rep

    def __repr__(self) -> str:
        return self._repr()


def find_step(step_object: Step, step_name: str) -> Optional[Step]:
    if step_object.start_msg == step_name:
        return step_object
    for child in step_object.steps.values():
        result = find_step(step_object=child, step_name=step_name)
        if result:
            return result
    return None


def indent_str(indent: int) -> str:
    return "===" * indent


def log_unlogged_steps(step: Step) -> list[str]:
    """
    Recursively collects log strings for all steps not yet marked as logged externally.
    Returns:
        A list of strings like 'step_name: 0.1234s' in execution order.
    """
    logs = []

    if not step.logged_external:
        indent = step.level - 1
        logs.append(f"{indent_str(indent)}  {step.end_msg} {step.timer.elapsed:.2f}s")
        step.logged_external = True

    for substep in step.steps.values():
        logs.extend(log_unlogged_steps(substep))

    return logs


def log_step_starting(step: Step) -> str:
    indent = step.level - 1
    return f"{indent_str(indent)}  {step.start_msg}"
