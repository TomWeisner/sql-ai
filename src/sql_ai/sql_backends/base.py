from __future__ import annotations

from typing import Protocol, Sequence

import pandas as pd

from sql_ai.sql_backends.table import Table
from sql_ai.sql_formatting.formatting import SQLFormatting, SQLFormattingOutput


class SqlBackend(Protocol):
    """
    Contract for SQL engines that the `SqlLLM` runner can orchestrate.

    Backends encapsulate all specific sql enginer behavior so the runner can
    remain enginer agnostic. Engines being e.g. Athena, MySQL, Redshift etc.
    To implement a new engine, create a class satisfying this protocol.

    Required members:
      * name - user facing label (e.g. used in logs/UI spinners).
      * tables - list of Table metadata objects supplied to prompts/formatters.
      * sql_formatter - instance of SQLFormatting configured with the
        backend's cleaning steps (ensures generated SQL matches dialect).
      * prompt_context_template / prompt_guidelines - text injected into
        prompts so the LLM knows how to build SQL.
      * populate_schemas - fetch/augment Table schemas before prompting.
      * describe_metadata_tables - surface system tables (e.g., information_schema)
        referenced by a query.
      * format_query -  engine specific sanitisation on generated SQL.
      * run_query - execute SQL and return rows as a DataFrame.
    """

    name: str
    tables: list[Table]
    sql_formatter: SQLFormatting
    prompt_context_template: str
    prompt_guidelines: str

    def populate_schemas(self) -> Sequence[Table]:
        """
        Populate each Table with dialect-specific schema info.

        Called before prompting to ensure the LLM sees current table schemas.
        Should mutate/return `self.tables`.
        """

    def describe_metadata_tables(self, sql: str, tables: list[Table]) -> list[Table]:
        """
        Add Table entries for metadata/system tables referenced in `sql`.

        Invitations to describe `information_schema` or other catalog tables
        are backend-specific, so each engine should detect and register its own.
        """

    def format_query(self, sql: str) -> SQLFormattingOutput:
        """
        Apply engine specific cleaning/validation to generated sql.

        Use `self.sql_formatter` (preconfigured with cleaning steps) to enforce
        dialect compliance and log each fix.
        """

    def run_query(self, query: str) -> pd.DataFrame:
        """
        Execute SQL and return results as a DataFrame.

        Implementation should handle any dialect specifics (sessions,
        error surfacing etc).
        """
