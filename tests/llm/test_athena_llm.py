from unittest.mock import patch

from sql_ai.athena.athena_llm import AthenaLLM
from sql_ai.athena.sql_prompting import (
    SQLPrompt,
)
from sql_ai.athena.table import Table

from sql_ai.streamlit.config_dataclass import Config


def test_athena_llm_instantiation():
    test_table = Table(
        name="station_lookup",
        description="test",
        catalog=None,
        database="default",
    )

    llm = AthenaLLM(tables=[test_table], config=Config())

    assert llm.tables[0].name == "station_lookup"
    assert llm.tables[0].catalog == "awsdatacatalog"
    assert len(llm.tables) == 1
    assert llm.max_tokens == 2000
    assert llm.athena_client.meta.service_model.service_name == "athena"
    assert llm.bedrock_runtime_client.meta.service_model.service_name == "bedrock-runtime"


def test_no_tables_supplied(mock_bedrock_client):
    """
    Tests that if no tables are supplied to generate_prompt_sql, it will only
    return a message indicating that no tables are found.
    """
    query, prompt, format_logs, error_trace = SQLPrompt().generate_sql(
        user_question="What is the average price of a car?",
        tables=[],
        bedrock_runtime_client=mock_bedrock_client,
    )
    assert query == "No tables found - unable to generate query."
    assert not prompt
    assert not format_logs


@patch("sql_ai.athena.sql_prompting.call_model_direct")
def test_one_table_supplied(mock_call_model_direct, mock_bedrock_client):
    """
    Tests that if one table is supplied to generate_prompt_sql, it will generate
    a query that includes the table name and relates to the users question.
    """
    mock_call_model_direct.return_value = """
        SELECT AVG("price")
        FROM "AwsDataCatalog"."default"."cars" as "c"
    """
    table = Table(
        name="cars",
        description="A table of cars",
        catalog="AwsDataCatalog",
        database="default",
    )
    sql_prompt = SQLPrompt()

    query, prompt, format_logs, error_trace = sql_prompt.generate_sql(
        user_question="What is the average price of a car?", tables=[table]
    )
    assert 'FROM "AwsDataCatalog"."default"."cars" as "c"'.lower() in query.lower()
    assert '"price"'.lower() in query.lower()
    assert prompt
    assert not error_trace


@patch("sql_ai.athena.sql_prompting.call_model_direct")
def test_one_table_supplied_custom_schema(mock_call_model_direct, mock_bedrock_client):
    """
    Tests that if one table is supplied to generate_prompt_sql that has a
    custom schema defined, it will generate
    a query that utilises the schema supplied.
    """
    mock_call_model_direct.return_value = """
        SELECT AVG("cost")
        FROM "AwsDataCatalog"."default"."cars" as "c"
    """
    table = Table(
        name="cars",
        description="A table of cars and how much they cost",
        catalog="AwsDataCatalog",
        database="default",
        schema={"cost": "float", "model": "string"},
    )
    sql_prompt = SQLPrompt()

    query, prompt, format_logs, error_trace = sql_prompt.generate_sql(
        user_question="What is the average price of a car?", tables=[table]
    )

    assert 'FROM "AwsDataCatalog"."default"."cars" as "c"'.lower() in query.lower()
    assert '"cost"'.lower() in query.lower()
    assert prompt
    assert not error_trace
