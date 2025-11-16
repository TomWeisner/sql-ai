from sql_ai.tracking.step import Step


class StepTracker:
    def __init__(self):
        self._root = Step(start_msg="root", level=0)
        self._stack = [self._root]

    @property
    def root(self):
        return self._root

    @property
    def current(self):
        return self._stack[-1]

    def push(self, step: Step):
        parent = self.current
        step.level = parent.level + 1
        parent.add_step(step)
        self._stack.append(step)

    def pop(self):
        self._stack.pop()

    def reset(self):
        self._root = Step(start_msg="root")
        self._stack = [self._root]


# GLOBAL instance
step_tracker = StepTracker()
