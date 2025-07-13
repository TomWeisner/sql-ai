import boto3
import pytest

from sql_ai.athena.sql_prompting import (
    SQLPrompt,
)
from sql_ai.athena.table import Table
from sql_ai.utils.utils import find_aws_profile_by_account_id


def instantiate_sql_prompt():
    session = boto3.Session(profile_name=find_aws_profile_by_account_id("382901073838"))
    bedrock_runtime_client = session.client("bedrock-runtime", region_name="eu-west-2")
    sql_prompt = SQLPrompt(bedrock_runtime_client=bedrock_runtime_client)
    return sql_prompt


@pytest.mark.local_only
def test_no_tables_supplied():
    """
    Tests that if no tables are supplied to generate_prompt_sql, it will only
    return a message indicating that no tables are found.
    """
    sql_prompt = instantiate_sql_prompt()
    query, prompt, format_logs, error_trace = sql_prompt.generate_sql(
        user_question="What is the average price of a car?", tables=[]
    )
    assert query == "No tables found - unable to generate query."
    assert not prompt
    assert not format_logs


@pytest.mark.local_only
def test_one_table_supplied():
    """
    Tests that if one table is supplied to generate_prompt_sql, it will generate
    a query that includes the table name and relates to the users question.
    """
    table = Table(
        name="cars",
        description="A table of cars",
        catalog="AwsDataCatalog",
        database="default",
    )
    sql_prompt = instantiate_sql_prompt()

    query, prompt, format_logs, error_trace = sql_prompt.generate_sql(
        user_question="What is the average price of a car?", tables=[table]
    )
    assert 'FROM "AwsDataCatalog"."default"."cars" as "c"'.lower() in query.lower()
    assert '"price"'.lower() in query.lower()
    assert prompt
    assert not error_trace


@pytest.mark.local_only
def test_one_table_supplied_custom_schema():
    """
    Tests that if one table is supplied to generate_prompt_sql that has a
    custom schema defined, it will generate
    a query that utilises the schema supplied.
    """
    table = Table(
        name="cars",
        description="A table of cars and how much they cost",
        catalog="AwsDataCatalog",
        database="default",
        schema={"cost": "float", "model": "string"},
    )
    sql_prompt = instantiate_sql_prompt()

    query, prompt, format_logs, error_trace = sql_prompt.generate_sql(
        user_question="What is the average price of a car?", tables=[table]
    )

    assert 'FROM "AwsDataCatalog"."default"."cars" as "c"'.lower() in query.lower()
    assert '"cost"'.lower() in query.lower()
    assert prompt
    assert not error_trace
