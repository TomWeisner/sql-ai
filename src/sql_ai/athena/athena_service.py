import re
import time
from typing import Any, Optional, Sequence

import boto3
import pandas as pd
from mypy_boto3_athena import AthenaClient

from sql_ai.athena.sql_formatting.formatting import SQLFormatting, SQLFormattingOutput
from sql_ai.athena.table import Table
from sql_ai.tracking.decorator import track_step_and_log


class AthenaService:
    def __init__(
        self,
        output_bucket: str,
        tables: Optional[Sequence[Table]] = None,
        client: Optional[AthenaClient] = None,
        database: str = "default",
        catalog: str = "awsdatacatalog",
        aws_profile: str = "",
        aws_region: str = "eu-west-2",
        wait_poll_interval: float = 1.0,
    ):
        if client is None:
            session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
            client = session.client("athena")

        self.client: AthenaClient = client  # type: ignore[assignment]
        self.output_bucket = output_bucket
        self.tables: list[Table] = list(tables) if tables else []
        self.database = database
        self.catalog = catalog
        self.wait_poll_interval = wait_poll_interval

    # ---------- Public API ----------

    def format_query(self, sql: str) -> SQLFormattingOutput:
        return SQLFormatting().format_sql(sql, tables=self.tables)

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
            print(f"Populating schema for {t}")
            if getattr(t, "schema", None) is None:
                t.schema = self.get_schema_from_athena(t)
        return self.tables

    @track_step_and_log(lambda self, table_name, **__: f"Table: {table_name}")
    def show_create_table(self, table_name: str) -> str:
        query = f"SHOW CREATE TABLE {table_name}"
        print(f"Running query: {query}")
        rows = self._fetch_results(query=query)
        # skip header; each following row has the single DDL string
        return "\n".join((row[0] or "") for row in rows[1:])

    def get_schema_from_athena(self, table: Table) -> str:
        """Extract raw DDL from Athena and strip lines that don't help SQL generation."""
        if not table.name:
            raise ValueError("Table name is required")
        if not table.database:
            raise ValueError("Table database is required")
        if not table.catalog:
            raise ValueError("Table catalog is required")

        ddl = self.show_create_table(table.name)

        uninteresting = {
            "CLUSTERED_BY",
            "ROW",
            "STORED",
            "WITH",
            "LOCATION",
            "TBLPROPERTIES",
            "OUTPUTFORMAT",
            "PARTITIONED",
        }

        lines_out: list[str] = []
        current_block = ""

        for line in ddl.splitlines():
            kw = self._get_starting_capitalized_word(line) or ""
            if kw and kw in uninteresting:
                current_block = kw
                continue
            if not kw and current_block in uninteresting:
                continue
            lines_out.append(line.strip())

        cleaned = "\n".join(lines_out).strip()
        return (
            cleaned.replace("CREATE TABLE", "")
            .replace("CREATE EXTERNAL TABLE", "")
            .strip()
        )

    # ---------- Internals ----------

    def _fetch_results(
        self, *, query: str, limit: Optional[int] = None
    ) -> list[list[Any]]:
        q = f"{query} LIMIT {limit}" if limit else query
        output_path = f"s3://{self.output_bucket}/"

        resp = self.client.start_query_execution(
            QueryString=q,
            QueryExecutionContext={"Database": self.database, "Catalog": self.catalog},
            ResultConfiguration={"OutputLocation": output_path},
        )
        print(f"Query execution started: {resp['QueryExecutionId']}")
        execution_id = resp["QueryExecutionId"]

        # Poll for completion
        while True:
            qexec = self.client.get_query_execution(QueryExecutionId=execution_id)
            status = qexec["QueryExecution"]["Status"]["State"]
            if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
                break
            time.sleep(self.wait_poll_interval)

        if status != "SUCCEEDED":
            reason = qexec["QueryExecution"]["Status"].get(
                "StateChangeReason", "No reason provided"
            )
            raise RuntimeError(
                f"Athena query failed with status: {status}\nReason: {reason}"
            )

        # Paginate results
        rows: list[list[Any]] = []
        next_token: Optional[str] = None
        first_page = True

        while True:
            page = (
                self.client.get_query_results(
                    QueryExecutionId=execution_id, NextToken=next_token
                )
                if next_token
                else self.client.get_query_results(QueryExecutionId=execution_id)
            )
            result_set = page["ResultSet"]
            result_rows = result_set["Rows"]

            if first_page:
                first_page = False
                cols = [c["Label"] for c in result_set["ResultSetMetadata"]["ColumnInfo"]]
                rows.append(cols)

            for r in result_rows:
                vals = [self._parse_value(f.get("VarCharValue", "")) for f in r["Data"]]
                rows.append(vals)

            next_token = page.get("NextToken")
            if not next_token:
                break

        return rows

    @staticmethod
    def _get_starting_capitalized_word(line: str) -> Optional[str]:
        m = re.match(r"^\s*([A-Z][a-zA-Z_]*)\b", line)
        return m.group(1) if m else None

    @staticmethod
    def _parse_value(value: str) -> Any:
        try:
            return int(value)
        except ValueError:
            try:
                return float(value)
            except ValueError:
                return value
