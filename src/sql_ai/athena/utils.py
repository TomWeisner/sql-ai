import re
import time
from typing import Any, Optional

import boto3
import pandas as pd
from mypy_boto3_athena import AthenaClient

from sql_ai.athena.table import Table
from sql_ai.streamlit.utils import track_step_and_log


def get_starting_capitalized_word(line: str) -> Optional[str]:
    """
    Returns the first capitalized word at the beginning of the line,
    or None if the line does not start with one.

    A capitalized word is defined as starting with A–Z and
    followed by letters/underscores.
    """
    match = re.match(r"^\s*([A-Z][a-zA-Z_]*)\b", line)
    return match.group(1) if match else None


def parse_value(value: str) -> Any:
    """
    Attempt to parse a value as an int or float, otherwise return as str.

    :param value: The value to parse
    :return: The parsed value, or the original string if not parseable
    """
    try:
        return int(value)
    except ValueError:
        try:
            return float(value)
        except ValueError:
            return value


def fetch_athena_results(
    client: AthenaClient,
    query: str,
    limit: Optional[int] = None,
    database: str = "default",
    catalog: str = "awsdatacatalog",
    output_bucket: str = "",
    aws_profile: str = "",
    aws_region: str = "eu-west-2",
    wait_poll_interval: float = 1.0,
) -> list[list[str]]:
    """
    Run an Athena query and return raw results as list of rows.
    Each row is a list of column values except the first row,
    which is the column names.

    :return: List of rows, each a list of column values (strings or None)
    """

    if client is None:
        session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
        client = session.client("athena")

    if limit:
        query += f" LIMIT {limit}"

    output_path = f"s3://{output_bucket}/"

    print(output_path)
    print(query)

    response = client.start_query_execution(
        QueryString=query,
        QueryExecutionContext={"Database": database, "Catalog": catalog},
        ResultConfiguration={"OutputLocation": output_path},
    )

    execution_id = response["QueryExecutionId"]

    # Wait for completion
    while True:
        result = client.get_query_execution(QueryExecutionId=execution_id)
        status = result["QueryExecution"]["Status"]["State"]
        if status in {"SUCCEEDED", "FAILED", "CANCELLED"}:
            break
        time.sleep(wait_poll_interval)

    if status != "SUCCEEDED":
        reason = result["QueryExecution"]["Status"].get(
            "StateChangeReason", "No reason provided"
        )
        raise Exception(f"Athena query failed with status: {status}\nReason: {reason}")

    # Fetch paginated results
    rows = []
    next_token = None
    is_first_page = True

    while True:
        if next_token:
            result_page = client.get_query_results(
                QueryExecutionId=execution_id, NextToken=next_token
            )
        else:
            result_page = client.get_query_results(QueryExecutionId=execution_id)

        result_set = result_page["ResultSet"]
        result_rows = result_set["Rows"]

        # Detect whether to skip header
        if is_first_page:
            is_first_page = False
            column_info = result_set["ResultSetMetadata"]["ColumnInfo"]
            column_labels = [col["Label"] for col in column_info]
            rows.append(column_labels)
            result_rows = result_rows[1:]

        for row in result_rows:
            values = [parse_value(field.get("VarCharValue", "")) for field in row["Data"]]
            rows.append(values)

        next_token = result_page.get("NextToken")
        if not next_token:
            break

    return rows


@track_step_and_log(lambda table_name, **_: f"Table: {table_name}")
def show_create_table(
    table_name: str,
    database: str,
    catalog: str,
    athena_client: AthenaClient,
    output_bucket: str,
) -> str:
    query = f"SHOW CREATE TABLE {table_name}"
    rows = fetch_athena_results(
        query=query,
        client=athena_client,
        database=database,
        catalog=catalog,
        output_bucket=output_bucket,
    )
    return "\n".join(row[0] or "" for row in rows[1:])


def get_schema_from_athena(athena_client: AthenaClient, table: Table, output_bucket: str):
    """Extract raw DDL from Athena, excluding metadata that does
    not facilitate SQL generation."""
    if table.name is None:
        raise ValueError("Table name is required")
    if table.database is None:
        raise ValueError("Table database is required")
    if table.catalog is None:
        raise ValueError("Table catalog is required")

    ddl = show_create_table(
        table.name,
        database=table.database,
        catalog=table.catalog,
        athena_client=athena_client,
        output_bucket=output_bucket,
    )
    uninteresting_properties = [
        "CLUSTERED_BY",
        "ROW",
        "STORED",
        "WITH",
        "LOCATION",
        "TBLPROPERTIES",
        "OUTPUTFORMAT",
        "PARTITIONED",
    ]
    output = "\n"
    current_key_word = ""
    for line in ddl.splitlines():
        key_word = get_starting_capitalized_word(line)
        if key_word is not None and key_word in uninteresting_properties:
            current_key_word = key_word
            continue
        elif key_word is None and current_key_word in uninteresting_properties:
            continue
        output += line.strip() + "\n"
    return output.strip().replace("CREATE TABLE", "").replace("CREATE EXTERNAL TABLE", "")


def run_query(
    query: str,
    limit: Optional[int] = None,
    database: str = "default",
    catalog: str = "awsdatacatalog",
    client: Any = None,
    output_bucket: str = "",
) -> pd.DataFrame:

    rows = fetch_athena_results(
        query=query,
        limit=limit,
        database=database,
        catalog=catalog,
        client=client,
        output_bucket=output_bucket,
    )
    if not rows:
        return pd.DataFrame()

    if len(rows) == 1 and len(rows[0]) == 1:
        return pd.DataFrame(rows, columns=["_col0"])

    return pd.DataFrame(rows[1:], columns=rows[0])
