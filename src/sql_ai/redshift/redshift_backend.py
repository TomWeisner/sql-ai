from __future__ import annotations

import time
from typing import TYPE_CHECKING, Any, Optional, Sequence

import boto3
import pandas as pd

from sql_ai.redshift.prompt_defaults import (
    REDSHIFT_CONTEXT_TEMPLATE,
    REDSHIFT_GUIDELINES,
)
from sql_ai.redshift.sql_formatting import SQLRedshiftCompliance, SQLRedshiftStandards
from sql_ai.sql_backend.base import SqlBackend
from sql_ai.sql_backend.table import Table
from sql_ai.sql_formatting.formatting import SQLFormatting, SQLFormattingOutput
from sql_ai.tracking.decorator import track_step_and_log

if TYPE_CHECKING:  # pragma: no cover
    from typing import Protocol

    class RedshiftDataAPIServiceClient(Protocol):
        def describe_table(self, **kwargs: Any) -> dict[str, Any]: ...
        def execute_statement(self, **kwargs: Any) -> dict[str, Any]: ...
        def describe_statement(self, **kwargs: Any) -> dict[str, Any]: ...
        def get_statement_result(self, **kwargs: Any) -> dict[str, Any]: ...

else:
    RedshiftDataAPIServiceClient = Any


class RedshiftBackend(SqlBackend):
    """Implementation of SqlBackend that talks to Amazon Redshift (Data API)."""

    name = "Redshift"

    def __init__(
        self,
        tables: Optional[Sequence[Table]] = None,
        client: Optional["RedshiftDataAPIServiceClient"] = None,
        database: str = "dev",
        cluster_identifier: str = "",
        workgroup_name: str = "",
        db_user: str = "",
        secret_arn: str = "",
        aws_profile: str = "",
        aws_region: str = "eu-west-2",
        wait_poll_interval: float = 1.0,
    ):
        if client is None:
            session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
            client = session.client("redshift-data")

        self.client: RedshiftDataAPIServiceClient = client  # type: ignore[assignment]
        self.tables: list[Table] = list(tables) if tables else []
        self.database = database
        self.cluster_identifier = cluster_identifier
        self.workgroup_name = workgroup_name
        self.db_user = db_user
        self.secret_arn = secret_arn
        self.wait_poll_interval = wait_poll_interval
        self.sql_formatter = SQLFormatting(
            [
                ("Redshift compliance", SQLRedshiftCompliance()),
                ("SQL standards", SQLRedshiftStandards()),
            ],
            metadata_describer=self,
        )
        self.prompt_context_template = REDSHIFT_CONTEXT_TEMPLATE
        self.prompt_guidelines = REDSHIFT_GUIDELINES

    def format_query(self, sql: str) -> SQLFormattingOutput:
        return self.sql_formatter.format_sql(sql, tables=self.tables)

    def describe_metadata_tables(self, sql: str, tables: list[Table]) -> list[Table]:
        lowered = sql.lower()
        if "information_schema" in lowered and "columns" in lowered:
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

    def run_query(self, query: str, limit: Optional[int] = None) -> pd.DataFrame:
        rows = self._fetch_results(query=query, limit=limit)
        if not rows:
            return pd.DataFrame()
        if len(rows) == 1 and len(rows[0]) == 1:
            return pd.DataFrame(rows, columns=["_col0"])
        return pd.DataFrame(rows[1:], columns=rows[0])

    @track_step_and_log("🔍 Getting schemas for tables")
    def populate_schemas(self) -> Sequence[Table]:
        for t in self.tables:
            if getattr(t, "schema", None) is None:
                t.schema = self.get_schema_from_redshift(t)
        return self.tables

    def get_schema_from_redshift(self, table: Table) -> dict[str, str]:
        params = self._common_request_kwargs()
        params.update({"Schema": table.database, "Table": table.name})

        columns: dict[str, str] = {}
        next_token: Optional[str] = None

        while True:
            req = params.copy()
            if next_token:
                req["NextToken"] = next_token
            resp = self.client.describe_table(**req)
            for col in resp.get("ColumnList", []):
                name = col.get("name") or col.get("Name")
                type_name = col.get("typeName") or col.get("type_name") or ""
                if name:
                    columns[name] = type_name
            next_token = resp.get("NextToken")
            if not next_token:
                break

        return columns

    def _common_request_kwargs(self) -> dict[str, str]:
        kwargs: dict[str, str] = {"Database": self.database}
        if self.workgroup_name:
            kwargs["WorkgroupName"] = self.workgroup_name
        elif self.cluster_identifier:
            kwargs["ClusterIdentifier"] = self.cluster_identifier
        if self.db_user:
            kwargs["DbUser"] = self.db_user
        if self.secret_arn:
            kwargs["SecretArn"] = self.secret_arn
        return kwargs

    def _fetch_results(
        self, *, query: str, limit: Optional[int] = None
    ) -> list[list[Any]]:
        q = f"{query} LIMIT {limit}" if limit else query
        params = self._common_request_kwargs()

        resp = self.client.execute_statement(Sql=q, **params)
        statement_id = resp["Id"]

        while True:
            desc = self.client.describe_statement(Id=statement_id)
            status = desc["Status"]
            if status in {"FINISHED", "FAILED", "ABORTED"}:
                break
            time.sleep(self.wait_poll_interval)

        if status != "FINISHED":
            reason = desc.get("Error", "No reason provided")
            raise RuntimeError(
                f"Redshift query failed with status: {status}\n"
                f"Reason: {reason}\n"
                f"Query:\n{q}"
            )

        rows: list[list[Any]] = []
        next_token: Optional[str] = None
        first_page = True

        while True:
            request_kwargs = {"Id": statement_id}
            if next_token:
                request_kwargs["NextToken"] = next_token

            page = self.client.get_statement_result(**request_kwargs)
            if first_page:
                first_page = False
                cols = [
                    (c.get("label") or c.get("name") or "").strip()
                    for c in page.get("ColumnMetadata", [])
                ]
                rows.append(cols)

            for record in page.get("Records", []):
                vals = [self._parse_value(field) for field in record]
                rows.append(vals)

            next_token = page.get("NextToken")
            if not next_token:
                break

        return rows

    @staticmethod
    def _parse_value(field: dict[str, Any]) -> Any:
        if not field:
            return None
        if field.get("isNull"):
            return None
        for key in ("stringValue", "longValue", "doubleValue", "booleanValue"):
            if key in field:
                return field[key]
        # Return whichever value is present for less common types.
        return next(iter(field.values()), None)
