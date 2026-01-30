import re
from dataclasses import dataclass
from typing import Protocol, Sequence, Tuple

from sql_ai.sql_backend.table import Table
from sql_ai.sql_formatting.formatter_base import SQLFormatter
from sql_ai.tracking.decorator import track_step_and_log


class MetadataDescriber(Protocol):
    def describe_metadata_tables(self, sql: str, tables: list[Table]) -> list[Table]: ...


@dataclass
class SQLFormattingOutput:
    formatted_sql: str
    logs: list[str]
    error_trace: str


class SQLFormatting:
    """Formatting logic (hand holding) for supplied SQL queries."""

    def __init__(
        self,
        formatters: Sequence[Tuple[str, SQLFormatter]] | None = None,
        metadata_describer: "MetadataDescriber | None" = None,
    ):
        self.formatters: list[tuple[str, SQLFormatter]] = list(formatters or [])
        self.metadata_describer = metadata_describer

    @track_step_and_log("🎨 Formatting SQL")
    def format_sql(self, sql: str, tables: list[Table]) -> SQLFormattingOutput:
        """
        Formats a SQL query to ensure compatibility with Amazon
        Athena and compliance with internal SQL standards.

        This method applies a sequence of formatting transformations to:
        - Correct syntax or structure that may cause Athena-specific issues
        - Enforce project-wide SQL coding conventions
        - Track and log each transformation applied

        Parameters:
        ----------
        sql : str
            The raw SQL query to be formatted.
        tables : list[Table]
            A list of Table objects that the SQL query references.

        Returns:
        -------
        tuple[str, list[str], str]
            - Formatted SQL query (str)
            - List of formatting step logs (list[str])
            - Error trace, if any occurred during formatting (str)
        """
        tables = tables.copy()
        error_trace = ""

        tables = self._find_generated_with_tables(sql, tables)
        if self.metadata_describer:
            tables = self.metadata_describer.describe_metadata_tables(sql, tables)

        sql = self._remove_encapsulating_quotes(sql)

        self.format_logs = ["Originally generated SQL:\n\n" + sql]

        for formatter_action, formatter in self.formatters:
            print(f"Applying {formatter_action}")
            if error_trace:
                break
            sql, logs, error_trace = formatter.format_sql(sql, tables)
            self.format_logs.append(
                f"\nApplying {formatter_action}:\n\n"
                + "\n".join(logs)
                + "\n\n--->\n\n"
                + sql
            )

        total_edits = sum(
            getattr(formatter, "total_edits", 0) for _, formatter in self.formatters
        )
        self.format_logs.append(f"Total edits applied: {total_edits}")

        print("SQL formatting complete")

        return SQLFormattingOutput(
            formatted_sql=sql,
            logs=self.format_logs,
            error_trace=error_trace,
        )

    def _find_generated_with_tables(self, sql: str, tables: list[Table]) -> list[Table]:
        """
        Find the tables that are created during SQL generation by looking for
        WITH statements (or subsequent `), tbl AS` statements) and adding any
        found tables to the list of tables.

        :param sql: The SQL query to check
        :param tables: The list of tables to add found tables to
        :return: The list of tables
        """
        with_table_pattern = r"\bWITH\s+(\w+)"
        additional_with_table_pattern = r"\),\s*(\w+)\s+AS\s*\(\s*SELECT"

        table_names = re.findall(with_table_pattern, sql)
        table_names.extend(re.findall(additional_with_table_pattern, sql))

        for table_name in table_names:
            new_table = Table(
                database="_",
                name=table_name,
                catalog="_",
                description="With table made during SQL generation",
            )
            tables.append(new_table)

        return tables

    def _remove_encapsulating_quotes(self, sql: str) -> str:
        s = sql.strip()
        wrappers = ("`", "'", '"', "`")

        stripped = False
        while not stripped:
            for w in wrappers:
                if s.startswith(w) and s.endswith(w):
                    print("Removind wrappers", w)
                    s = s[1:-1].strip()
                else:
                    stripped = True
        return s
