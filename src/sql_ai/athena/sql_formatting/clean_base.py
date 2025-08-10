import re
import string
import traceback
from abc import ABC
from collections import Counter, defaultdict
from functools import wraps
from typing import Callable

from sql_ai.athena.table import Table
from sql_ai.tracking.decorator import track_step_and_log


class SQLCleaning(ABC):
    """Cleansing logic (hand holding) for supplied SQL queries"""

    def __init__(self, nickname: str = "") -> None:
        self.replacement_count: int = 1
        self.log_entries: list[str] = []
        self.pending_logs: list = []
        self.methods: list[Callable] = []
        self.nickname = nickname
        assert self.nickname

    def _reset_logs(self):
        self.log_entries.clear()
        self.replacement_count = 1
        self.pending_logs.clear()

    def log_replacements(
        self,
        before_list: list[str],
        after_list: list[str],
        message: str = "",
        indent_spaces: int = 2,
    ):
        """
        Logs a set of before and after strings,
        along with an optional message and indent level.

        The output is a string of the form:
        <count>. <message>
            <indent>a. <before> -> <after> (applied x<times> times)
            <indent>b. <before> -> <after>
            <indent>c. <before> -> <after>

        Where:
        - <count> is a monotonically increasing number for each call
        - <message> is the provided message
        - <indent> is the provided indent level
        - <before> and <after> are the corresponding elements of the
          before_list and after_list
        - <times> is the number of times the replacement was applied,
        only shows if times > 1

        If the before_list and after_list are empty, or if the before_list
        and after_list are the same
        (i.e. no replacements were made), nothing is logged.

        :param before_list: List of strings to replace
        :param after_list: List of strings to replace with
        :param message: Optional message to include in the log
        :param indent_spaces: Number of spaces to indent the log message
        """
        if before_list == after_list:
            return

        if not before_list:
            return

        indent = " " * indent_spaces

        def format_entry(before: str, after: str) -> str:
            return f"{before} -> {after}"

        # Prepare message header
        header = f"{self.replacement_count}. {message}"

        # Build the message body
        if len(before_list) != len(after_list):
            raise ValueError("Before and after lists must be the same length")

        formatted_lines = [
            format_entry(before, after)
            for before, after in zip(before_list, after_list)
            if before is not None
        ]
        line_counts = Counter(formatted_lines)
        # dedupe, whilst preserving order
        seen = set()
        unique_lines = []
        for line in formatted_lines:
            if line not in seen:
                seen.add(line)
                unique_lines.append(line)
        body_lines = []
        for i, line in enumerate(unique_lines):
            if line_counts[line] > 1:
                mutli_msg = (
                    f"{indent}{string.ascii_letters[i]}. {line} "
                    f"(applied x{line_counts[line]} times)"
                )
                body_lines.append(mutli_msg)
            else:
                body_lines.append(f"{indent}{string.ascii_letters[i]}. {line}")
        body = "\n".join(body_lines)

        # Combine and store
        message = header
        if body:
            message += f"\n{body}"
        self.log_entries.append(message)
        self.replacement_count += 1

    @staticmethod
    def auto_log_replacements(
        msg: str,
        flags=re.IGNORECASE | re.VERBOSE | re.DOTALL,
    ):
        """
        Decorator to log replacements made by a method.

        :param msg: Header message to include in the log
        :param flags: Flags to pass to re.sub
        :return: Decorated method
        """

        def decorator(fn: Callable):

            @wraps(fn)
            def wrapped(self, *args, **kwargs):

                sql = args[0]
                before_list, after_list = [], []
                pattern, replacer_func = fn(self, *args, **kwargs)

                def replacer(match: re.Match) -> str:
                    """
                    A wrapper around a replacement function, which logs the
                    replacements in a friendly format

                    The replacement function should return a tuple of (before, after_obj)
                    before can be a custom string to display before the
                    replacement in the log, and after_obj can be either a string (what
                    to replace with), or a list of two strings: [what to replace with,
                    what to display in the log]
                    """
                    before, after_obj = replacer_func(match)
                    after = after_custom = (
                        after_obj
                        if isinstance(after_obj, str)
                        else match.group(0)  # default to prevent type hinting errors
                    )
                    if isinstance(after_obj, str):
                        after = after_custom = after_obj
                    elif isinstance(after_obj, list):
                        after, after_custom = after_obj[0], after_obj[1]
                    before_list.append(before)
                    after_list.append(after_custom)
                    return after

                result = re.sub(pattern, replacer, sql, flags=flags)

                self.pending_logs.append((msg, before_list, after_list))
                return result

            return wrapped

        return decorator

    def log_pending_replacements(self):
        if hasattr(self, "pending_logs"):
            # maps msg → (befores, afters)
            grouped_logs = defaultdict(lambda: ([], []))
            # Group by msg (if same replacement type appears more than
            # once group all actions up)
            for msg, before_list, after_list in self.pending_logs:
                grouped_before_list, grouped_after_list = grouped_logs[msg]
                grouped_before_list.extend(before_list)
                grouped_after_list.extend(after_list)

            for msg, (
                grouped_before_list,
                grouped_after_list,
            ) in grouped_logs.items():
                if grouped_before_list:
                    self.log_replacements(
                        before_list=grouped_before_list,
                        after_list=grouped_after_list,
                        message=msg,
                    )
            self.pending_logs.clear()

    @track_step_and_log(lambda self, *_: self.nickname)
    def format_sql(self, sql: str, tables: list[Table]) -> tuple[str, list[str], str]:
        self._reset_logs()
        tables = tables.copy()
        error_trace = ""

        try:
            for method in self.methods:
                sql = method(sql, tables)
                self.log_pending_replacements()

        except Exception:
            error_trace = traceback.format_exc()
            print(error_trace)
        return sql, self.log_entries, error_trace
