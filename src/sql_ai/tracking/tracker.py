from sql_ai.athena.tracking.step import Step


class StepTracker:
    def __init__(self):
        self.root = Step(start_msg="root", level=0)
        self.stack = [self.root]

    def current(self):
        return self.stack[-1]

    def push(self, step: Step):
        parent = self.current()
        step.level = parent.level + 1
        parent.add_step(step)
        self.stack.append(step)

    def pop(self):
        self.stack.pop()

    def reset(self):
        self.root = Step(start_msg="root")
        self.stack = [self.root]


# GLOBAL instance
step_tracker = StepTracker()
