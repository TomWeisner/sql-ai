import re

from sql_ai.athena.clean_sql.cleaning_sql import (
    SQLCleaning,
)
from sql_ai.athena.clean_sql.sql_fixing import (
    SQLAthena,
)
from sql_ai.athena.clean_sql.sql_standardising import (
    SQLStandards,
)
from sql_ai.athena.table import Table
from sql_ai.streamlit.utils import track_step_and_log


class SQLFormatting:
    """Formatting logic (hand holding) for supplied SQL queries"""

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

    def _find_information_schema_tables(
        self, sql: str, tables: list[Table]
    ) -> list[Table]:
        """Find any references to information_schema.columns in
        the query and add it to the list of tables if found"""
        if '"information_schema"."columns"' in sql:
            description = (
                "Metadata about columns in tables. "
                "Note metadata is not a real catalog."
            )
            metadata_table = Table(
                database="information_schema",
                name="columns",
                catalog="_",
                description=description,
            )
            tables.append(metadata_table)
        return tables

    @track_step_and_log("🎨 Formatting SQL")
    def format_sql(self, sql: str, tables: list[Table]) -> tuple[str, list[str], str]:
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
        tables = self._find_information_schema_tables(sql, tables)

        self.format_logs = ["Originally generated SQL:\n\n" + sql]

        formatters: dict[str, SQLCleaning] = {
            "Athena fixing": SQLAthena(),
            "SQL standards": SQLStandards(),
        }

        for formatter_action, formatter in formatters.items():
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

        print("SQL formatting complete")
        return sql, self.format_logs, error_trace
